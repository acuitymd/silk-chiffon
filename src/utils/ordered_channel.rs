//! Ordered channel for async message passing with index-based ordering.
//!
//! Items can be sent out of order but are received in order. Senders specify
//! an index for each item, and the receiver delivers items in index order
//! (0, 1, 2, ...).
//!
//! Blocks until the next index is available when there is no more space in the buffer.

use std::{
    collections::BTreeMap,
    fmt::{self, Debug, Formatter},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures::Stream;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError<T> {
    Stale(T),
    Duplicate(T),
    Closed(T),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecvError;

struct Inner<T> {
    slots: BTreeMap<usize, T>,
    capacity: usize,
    next_index: usize,
}

enum InsertResult<T> {
    Success,
    TooFarAhead(T),
    Stale(T),
    Duplicate(T),
}

enum TakeNextResult<T> {
    Success(T),
    Empty,
}

/// Inner state of the ordered channel. Expects to be protected by a mutex.
impl<T> Inner<T> {
    fn new(capacity: usize) -> Self {
        Self {
            slots: BTreeMap::new(),
            capacity,
            next_index: 0,
        }
    }

    fn insert(&mut self, index: usize, item: T) -> InsertResult<T> {
        if index < self.next_index {
            return InsertResult::Stale(item);
        }

        if index >= self.next_index + self.capacity {
            return InsertResult::TooFarAhead(item);
        }

        if self.slots.contains_key(&index) {
            return InsertResult::Duplicate(item);
        }

        self.slots.insert(index, item);

        InsertResult::Success
    }

    fn take_next(&mut self) -> TakeNextResult<T> {
        if let Some(value) = self.slots.remove(&self.next_index) {
            self.next_index += 1;
            TakeNextResult::Success(value)
        } else {
            TakeNextResult::Empty
        }
    }

    fn next_index_available(&self) -> bool {
        self.slots.contains_key(&self.next_index)
    }
}

struct Shared<T> {
    inner: Mutex<Inner<T>>,
    sender_count: AtomicUsize,
    receiver_count: AtomicUsize,
    send_notify: Notify,
    recv_notify: Notify,
}

pub struct OrderedSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Debug for OrderedSender<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderedSender").finish_non_exhaustive()
    }
}

impl<T> Clone for OrderedSender<T> {
    fn clone(&self) -> Self {
        // a clone and a drop cannot happen at the same time for the same sender,
        // so the counter will have 1 for this self.shared already and concurrent
        // drops can't decrement it to 0
        self.shared.sender_count.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for OrderedSender<T> {
    fn drop(&mut self) {
        if self.shared.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.recv_notify.notify_waiters();
        }
    }
}

pub(crate) fn block_on<F: std::future::Future>(f: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            // block_in_place is a no-op when called from spawn_blocking,
            // but correctly handles being called from a worker thread
            tokio::task::block_in_place(|| handle.block_on(f))
        }
        Err(_) => futures::executor::block_on(f),
    }
}

impl<T> OrderedSender<T> {
    #[allow(dead_code)] // part of complete API, tested
    pub fn blocking_send(&self, index: usize, item: T) -> Result<(), SendError<T>> {
        block_on(self.send(index, item))
    }

    #[allow(dead_code)] // part of complete API, also tested
    pub async fn send_cancellable(
        &self,
        index: usize,
        item: T,
        cancel: &CancellationToken,
    ) -> Result<(), SendError<T>> {
        let mut item = item;

        loop {
            if cancel.is_cancelled() {
                return Err(SendError::Closed(item));
            }

            let notified = {
                let mut inner = self.shared.inner.lock().await;

                match inner.insert(index, item) {
                    InsertResult::Success => {
                        if index == inner.next_index {
                            self.shared.recv_notify.notify_one();
                        }
                        return Ok(());
                    }
                    InsertResult::TooFarAhead(returned_item) => {
                        item = returned_item;
                        let notified = self.shared.send_notify.notified();

                        if self.shared.receiver_count.load(Ordering::Acquire) == 0 {
                            return Err(SendError::Closed(item));
                        }

                        notified
                    }
                    InsertResult::Stale(returned_item) => {
                        return Err(SendError::Stale(returned_item));
                    }
                    InsertResult::Duplicate(returned_item) => {
                        return Err(SendError::Duplicate(returned_item));
                    }
                }
            };

            tokio::select! {
                _ = cancel.cancelled() => return Err(SendError::Closed(item)),
                _ = notified => {}
            }
        }
    }

    pub async fn send(&self, index: usize, item: T) -> Result<(), SendError<T>> {
        let mut item = item;

        loop {
            let notified = {
                let mut inner = self.shared.inner.lock().await;

                match inner.insert(index, item) {
                    InsertResult::Success => {
                        if index == inner.next_index {
                            // next_index only advances on take_next(), not on insert, so this
                            // checks if we just inserted the item the receiver is waiting for
                            self.shared.recv_notify.notify_one();
                        }
                        return Ok(());
                    }
                    InsertResult::TooFarAhead(returned_item) => {
                        item = returned_item;
                        // register notifier while we still have the lock to avoid race
                        let notified = self.shared.send_notify.notified();

                        if self.shared.receiver_count.load(Ordering::Acquire) == 0 {
                            return Err(SendError::Closed(item));
                        }

                        notified
                    }
                    InsertResult::Stale(returned_item) => {
                        return Err(SendError::Stale(returned_item));
                    }
                    InsertResult::Duplicate(returned_item) => {
                        return Err(SendError::Duplicate(returned_item));
                    }
                }
            };

            notified.await;
        }
    }
}

pub struct OrderedReceiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Debug for OrderedReceiver<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderedReceiver").finish_non_exhaustive()
    }
}

impl<T> Clone for OrderedReceiver<T> {
    fn clone(&self) -> Self {
        self.shared.receiver_count.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for OrderedReceiver<T> {
    fn drop(&mut self) {
        if self.shared.receiver_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.send_notify.notify_waiters();
        }
    }
}

impl<T> OrderedReceiver<T> {
    #[allow(dead_code)] // part of complete API, tested
    pub fn blocking_recv(&mut self) -> Result<T, RecvError> {
        block_on(self.recv())
    }

    pub fn blocking_recv_cancellable(
        &mut self,
        cancel: &CancellationToken,
    ) -> Result<T, RecvError> {
        block_on(async {
            tokio::select! {
                _ = cancel.cancelled() => Err(RecvError),
                result = self.recv() => result,
            }
        })
    }

    pub async fn recv(&mut self) -> Result<T, RecvError> {
        loop {
            let notified = {
                let mut inner = self.shared.inner.lock().await;

                match inner.take_next() {
                    TakeNextResult::Success(item) => {
                        // notify all senders since we don't know which has the next index
                        self.shared.send_notify.notify_waiters();

                        if inner.next_index_available() {
                            self.shared.recv_notify.notify_one();
                        }

                        return Ok(item);
                    }
                    TakeNextResult::Empty => {
                        // register notifier before checking sender count to avoid race
                        let notified = self.shared.recv_notify.notified();

                        if self.shared.sender_count.load(Ordering::Acquire) == 0 {
                            return Err(RecvError);
                        }

                        notified
                    }
                }
            };

            notified.await;
        }
    }

    #[allow(dead_code)] // part of complete API, tested
    pub fn into_stream(self) -> impl Stream<Item = T> {
        futures::stream::unfold(self, |mut rx| async {
            match rx.recv().await {
                Ok(item) => Some((item, rx)),
                Err(RecvError) => None,
            }
        })
    }
}

/// Create an ordered channel with the given capacity.
///
/// # Panics
/// Panics if capacity is 0.
pub fn ordered_channel<T>(capacity: usize) -> (OrderedSender<T>, OrderedReceiver<T>) {
    assert!(capacity > 0, "ordered_channel capacity must be > 0");
    let shared = Arc::new(Shared {
        inner: Mutex::new(Inner::new(capacity)),
        sender_count: AtomicUsize::new(1),
        receiver_count: AtomicUsize::new(1),
        send_notify: Notify::new(),
        recv_notify: Notify::new(),
    });

    (
        OrderedSender {
            shared: Arc::clone(&shared),
        },
        OrderedReceiver { shared },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_send_recv() {
        let (tx, mut rx) = ordered_channel::<i32>(4);

        tx.send(0, 10).await.unwrap();
        tx.send(1, 20).await.unwrap();

        assert_eq!(rx.recv().await.unwrap(), 10);
        assert_eq!(rx.recv().await.unwrap(), 20);
    }

    #[tokio::test]
    async fn test_out_of_order_send() {
        let (tx, mut rx) = ordered_channel::<i32>(4);

        tx.send(1, 20).await.unwrap();
        tx.send(0, 10).await.unwrap();
        tx.send(2, 30).await.unwrap();

        assert_eq!(rx.recv().await.unwrap(), 10);
        assert_eq!(rx.recv().await.unwrap(), 20);
        assert_eq!(rx.recv().await.unwrap(), 30);
    }

    #[tokio::test]
    async fn test_channel_closure() {
        let (tx, mut rx) = ordered_channel::<i32>(4);

        tx.send(0, 10).await.unwrap();
        drop(tx);

        assert_eq!(rx.recv().await.unwrap(), 10);
        assert!(rx.recv().await.is_err());
    }

    #[tokio::test]
    async fn test_stale_index() {
        let (tx, mut rx) = ordered_channel::<i32>(4);

        tx.send(0, 10).await.unwrap();
        rx.recv().await.unwrap();

        let result = tx.send(0, 20).await;
        assert!(matches!(result, Err(SendError::Stale(20))));
    }

    #[tokio::test]
    async fn test_duplicate_index() {
        let (tx, _rx) = ordered_channel::<i32>(4);

        tx.send(0, 10).await.unwrap();
        let result = tx.send(0, 20).await;
        assert!(matches!(result, Err(SendError::Duplicate(20))));
    }

    #[tokio::test]
    async fn test_multiple_senders() {
        let (tx1, mut rx) = ordered_channel::<i32>(4);
        let tx2 = tx1.clone();

        tx1.send(0, 10).await.unwrap();
        tx2.send(1, 20).await.unwrap();

        drop(tx1);

        assert_eq!(rx.recv().await.unwrap(), 10);
        assert_eq!(rx.recv().await.unwrap(), 20);

        drop(tx2);
        assert!(rx.recv().await.is_err());
    }

    #[tokio::test]
    async fn test_backpressure() {
        let (tx, mut rx) = ordered_channel::<i32>(2);

        tx.send(0, 10).await.unwrap();
        tx.send(1, 20).await.unwrap();

        let tx_clone = tx.clone();
        let handle = tokio::spawn(async move {
            tx_clone.send(2, 30).await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        assert_eq!(rx.recv().await.unwrap(), 10);

        handle.await.unwrap();

        assert_eq!(rx.recv().await.unwrap(), 20);
        assert_eq!(rx.recv().await.unwrap(), 30);
    }

    #[tokio::test]
    async fn test_stream() {
        use futures::StreamExt;

        let (tx, rx) = ordered_channel::<i32>(4);

        tx.send(1, 20).await.unwrap();
        tx.send(0, 10).await.unwrap();
        tx.send(2, 30).await.unwrap();
        drop(tx);

        let results: Vec<_> = rx.into_stream().collect().await;
        assert_eq!(results, vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn test_receiver_closure() {
        let (tx, rx) = ordered_channel::<i32>(2);

        tx.send(0, 10).await.unwrap();
        tx.send(1, 20).await.unwrap();

        drop(rx);

        let result = tx.send(2, 30).await;
        assert!(matches!(result, Err(SendError::Closed(30))));
    }

    #[tokio::test]
    async fn test_cancellation() {
        let (tx, _rx) = ordered_channel::<i32>(1);
        let cancel = CancellationToken::new();

        tx.send(0, 10).await.unwrap();

        let cancel_clone = cancel.clone();
        let tx_clone = tx.clone();
        let handle =
            tokio::spawn(async move { tx_clone.send_cancellable(1, 20, &cancel_clone).await });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        cancel.cancel();

        let result = handle.await.unwrap();
        assert!(matches!(result, Err(SendError::Closed(20))));
    }

    #[test]
    fn test_blocking_send_recv() {
        let (tx, mut rx) = ordered_channel::<i32>(4);

        let tx_clone = tx.clone();
        let handle = std::thread::spawn(move || {
            tx_clone.blocking_send(1, 20).unwrap();
            tx_clone.blocking_send(0, 10).unwrap();
        });

        handle.join().unwrap();
        drop(tx);

        assert_eq!(rx.blocking_recv().unwrap(), 10);
        assert_eq!(rx.blocking_recv().unwrap(), 20);
    }

    const _: () = {
        const fn assert_send<T: Send>() {}
        const fn assert_sync<T: Sync>() {}

        assert_send::<OrderedSender<i32>>();
        assert_sync::<OrderedSender<i32>>();
        assert_send::<OrderedReceiver<i32>>();
        assert_sync::<OrderedReceiver<i32>>();
    };
}
