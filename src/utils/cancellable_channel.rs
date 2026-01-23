//! Cancellable channel wrappers built on flume.
//!
//! Provides wrappers around flume channels that support graceful
//! cancellation via a shared CancellationToken.

use std::fmt::{self, Display, Formatter};

use flume::{Receiver, Sender, bounded, unbounded};
use tokio_util::sync::CancellationToken;

use crate::utils::blocking::block_on;

/// Error type for channel operations.
#[derive(Debug)]
pub enum ChannelError {
    /// Operation was cancelled via the cancellation token.
    Cancelled,
    /// Channel was disconnected (sender/receiver dropped).
    Disconnected {
        channel: &'static str,
        task: Option<String>,
    },
}

impl Display for ChannelError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => write!(f, "operation cancelled"),
            Self::Disconnected { channel, task } => {
                if let Some(task) = task {
                    write!(f, "channel '{}' disconnected (task: {})", channel, task)
                } else {
                    write!(f, "channel '{}' disconnected", channel)
                }
            }
        }
    }
}

impl std::error::Error for ChannelError {}

pub struct CancellableSender<T> {
    tx: Sender<T>,
    cancel_token: CancellationToken,
    channel_name: &'static str,
    task_name: Option<String>,
}

impl<T> CancellableSender<T> {
    pub fn new(tx: Sender<T>, cancel_token: CancellationToken, channel_name: &'static str) -> Self {
        Self {
            tx,
            cancel_token,
            channel_name,
            task_name: None,
        }
    }

    /// Set the task name for error reporting.
    pub fn with_task_name(mut self, name: impl Into<String>) -> Self {
        self.task_name = Some(name.into());
        self
    }

    pub async fn send(&self, value: T) -> Result<(), ChannelError> {
        tokio::select! {
            biased;
            _ = self.cancel_token.cancelled() => Err(ChannelError::Cancelled),
            result = self.tx.send_async(value) => {
                result.map_err(|_| ChannelError::Disconnected {
                    channel: self.channel_name,
                    task: self.task_name.clone(),
                })
            }
        }
    }

    pub fn blocking_send(&self, value: T) -> Result<(), ChannelError> {
        block_on(self.send(value))
    }
}

impl<T> Clone for CancellableSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            cancel_token: self.cancel_token.clone(),
            channel_name: self.channel_name,
            task_name: self.task_name.clone(),
        }
    }
}

impl<T> std::fmt::Debug for CancellableSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellableSender")
            .field("channel_name", &self.channel_name)
            .field("task_name", &self.task_name)
            .finish_non_exhaustive()
    }
}

pub struct CancellableReceiver<T> {
    rx: Receiver<T>,
    cancel_token: CancellationToken,
    channel_name: &'static str,
    task_name: Option<String>,
}

impl<T> CancellableReceiver<T> {
    pub fn new(
        rx: Receiver<T>,
        cancel_token: CancellationToken,
        channel_name: &'static str,
    ) -> Self {
        Self {
            rx,
            cancel_token,
            channel_name,
            task_name: None,
        }
    }

    /// Set the task name for error reporting.
    pub fn with_task_name(mut self, name: impl Into<String>) -> Self {
        self.task_name = Some(name.into());
        self
    }

    pub async fn recv(&mut self) -> Result<T, ChannelError> {
        tokio::select! {
            biased;
            _ = self.cancel_token.cancelled() => Err(ChannelError::Cancelled),
            result = self.rx.recv_async() => {
                result.map_err(|_| ChannelError::Disconnected {
                    channel: self.channel_name,
                    task: self.task_name.clone(),
                })
            }
        }
    }

    pub fn blocking_recv(&mut self) -> Result<T, ChannelError> {
        block_on(self.recv())
    }
}

impl<T> Clone for CancellableReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            rx: self.rx.clone(),
            cancel_token: self.cancel_token.clone(),
            channel_name: self.channel_name,
            task_name: self.task_name.clone(),
        }
    }
}

impl<T> std::fmt::Debug for CancellableReceiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellableReceiver")
            .field("channel_name", &self.channel_name)
            .field("task_name", &self.task_name)
            .finish_non_exhaustive()
    }
}

/// Create a cancellable bounded channel.
pub fn cancellable_channel_bounded<T>(
    capacity: usize,
    cancel_token: CancellationToken,
    channel_name: &'static str,
) -> (CancellableSender<T>, CancellableReceiver<T>) {
    let (tx, rx) = bounded::<T>(capacity);
    (
        CancellableSender::new(tx, cancel_token.clone(), channel_name),
        CancellableReceiver::new(rx, cancel_token, channel_name),
    )
}

/// Create a cancellable unbounded channel.
///
/// Warning: unbounded channels can grow without limit if the receiver
/// is slower than the sender. Use bounded channels when backpressure
/// is needed to prevent unbounded memory growth.
pub fn cancellable_channel_unbounded<T>(
    cancel_token: CancellationToken,
    channel_name: &'static str,
) -> (CancellableSender<T>, CancellableReceiver<T>) {
    let (tx, rx) = unbounded::<T>();
    (
        CancellableSender::new(tx, cancel_token.clone(), channel_name),
        CancellableReceiver::new(rx, cancel_token, channel_name),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_send_recv_basic() {
        let cancel_token = CancellationToken::new();
        let (tx, mut rx) = cancellable_channel_bounded(4, cancel_token, "test");

        tx.send(42).await.unwrap();
        let received = rx.recv().await.unwrap();
        assert_eq!(received, 42);
    }

    #[tokio::test]
    async fn test_cancelled_send() {
        let cancel_token = CancellationToken::new();
        let (tx, mut _rx) = cancellable_channel_bounded::<i32>(1, cancel_token.clone(), "test");

        cancel_token.cancel();

        let result = tx.send(42).await;
        assert!(matches!(result, Err(ChannelError::Cancelled)));
    }

    #[tokio::test]
    async fn test_cancelled_recv() {
        let cancel_token = CancellationToken::new();
        let (_tx, mut rx) = cancellable_channel_bounded::<i32>(4, cancel_token.clone(), "test");

        cancel_token.cancel();

        let result = rx.recv().await;
        assert!(matches!(result, Err(ChannelError::Cancelled)));
    }

    #[tokio::test]
    async fn test_disconnected_on_drop() {
        let cancel_token = CancellationToken::new();
        let (tx, mut rx) = cancellable_channel_bounded::<i32>(4, cancel_token.clone(), "test");

        drop(tx);

        let result = rx.recv().await;
        assert!(matches!(
            result,
            Err(ChannelError::Disconnected {
                channel: "test",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn test_task_name_in_recv_error() {
        let cancel_token = CancellationToken::new();
        let (tx, rx) = cancellable_channel_bounded::<i32>(4, cancel_token.clone(), "input");
        let mut rx = rx.with_task_name("reader-0");

        drop(tx);

        let result = rx.recv().await;
        match result {
            Err(ChannelError::Disconnected { channel, task }) => {
                assert_eq!(channel, "input");
                assert_eq!(task, Some("reader-0".to_string()));
            }
            _ => panic!("expected Disconnected error"),
        }
    }

    #[tokio::test]
    async fn test_task_name_in_send_error() {
        let cancel_token = CancellationToken::new();
        let (tx, rx) = cancellable_channel_bounded::<i32>(4, cancel_token.clone(), "output");
        let tx = tx.with_task_name("writer-0");

        drop(rx);

        let result = tx.send(42).await;
        match result {
            Err(ChannelError::Disconnected { channel, task }) => {
                assert_eq!(channel, "output");
                assert_eq!(task, Some("writer-0".to_string()));
            }
            _ => panic!("expected Disconnected error"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_blocking_send_recv() {
        let cancel_token = CancellationToken::new();
        let (tx, mut rx) = cancellable_channel_bounded::<i32>(4, cancel_token, "test");

        tx.blocking_send(42).unwrap();
        tx.blocking_send(43).unwrap();

        assert_eq!(rx.blocking_recv().unwrap(), 42);
        assert_eq!(rx.blocking_recv().unwrap(), 43);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_blocking_cancelled() {
        let cancel_token = CancellationToken::new();
        let (tx, mut rx) = cancellable_channel_bounded::<i32>(4, cancel_token.clone(), "test");

        cancel_token.cancel();

        assert!(matches!(tx.blocking_send(42), Err(ChannelError::Cancelled)));
        assert!(matches!(rx.blocking_recv(), Err(ChannelError::Cancelled)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_blocking_disconnected() {
        let cancel_token = CancellationToken::new();
        let (tx, rx) = cancellable_channel_bounded::<i32>(4, cancel_token, "test");

        drop(rx);
        assert!(matches!(
            tx.blocking_send(42),
            Err(ChannelError::Disconnected { .. })
        ));

        let cancel_token = CancellationToken::new();
        let (tx, mut rx) = cancellable_channel_bounded::<i32>(4, cancel_token, "test");

        drop(tx);
        assert!(matches!(
            rx.blocking_recv(),
            Err(ChannelError::Disconnected { .. })
        ));
    }
}
