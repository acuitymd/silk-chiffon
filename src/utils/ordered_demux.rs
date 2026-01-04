//! Stream extension for ordered demultiplexing with bounded concurrency.
//!
//! Routes stream items to workers by partition key, spawning workers as new
//! partitions arrive (respecting concurrency limits), and collecting outputs
//! in partition order.
//!
//! This was born out of a want for structured concurrency primitives for
//! processing a stream in parallel with bounded concurrency and ordered output.

use std::{
    collections::hash_map::Entry,
    fmt::{self, Debug, Formatter},
    future::Future,
    hash::Hash,
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use rustc_hash::FxHashMap;

use anyhow::Result;
use futures::{Stream, StreamExt, ready};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::{
    first_error::FirstError,
    ordered_channel::{self, OrderedReceiver, OrderedSender},
};

/// Trait for items that belong to a partition and have an ordering within that partition.
pub trait Partitioned {
    /// The key type used to identify partitions.
    type Key: Clone + Eq + Hash + Ord + Send;

    /// Returns the partition key for this item.
    fn partition_key(&self) -> Self::Key;

    /// Returns the sequence index within the partition (0, 1, 2, ...).
    fn sequence_index(&self) -> usize;

    /// Returns true if this is the last item in the partition.
    fn is_last(&self) -> bool;
}

/// Configuration for ordered demux.
#[derive(Debug, Clone)]
pub struct OrderedDemuxConfig {
    /// Maximum concurrent workers.
    pub max_concurrent: usize,
    /// Buffer size for batches sent to each worker.
    pub worker_buffer_size: usize,
    /// Buffer size for output collection.
    pub output_buffer_size: usize,
}

/// Extension trait for streams to enable ordered demultiplexing.
///
/// This lets us write code like this:
/// ```
/// let output = input.ordered_demux(config, cancel, |key, rx, cancel| async move {
///     let mut sum = 0;
///     while let Ok(item) = rx.recv().await {
///         sum += item.value;
///     }
///     Ok(sum)
/// }).await;
/// ```
///
/// Where `input` is a stream of items, `config` is a configuration for the demux,
/// `cancel` is a cancellation token, and `worker_factory` is a function that creates a worker for each partition.
///
/// The worker factory is a function that takes a partition key, an ordered receiver for the items in the partition,
/// and a cancellation token, and returns a future that produces the output for the partition.
///
pub trait OrderedDemuxExt: Stream + Sized {
    /// Demultiplex stream items by partition key, spawning workers for each partition.
    ///
    /// Items are routed to workers based on their `partition_key()`. When a new partition
    /// is encountered, a new worker is spawned (after acquiring a concurrency slot).
    /// Outputs are yielded in partition order regardless of which worker finishes first.
    fn ordered_demux<WorkerOutput, WorkerFactory, AwaitableWorkerOutput>(
        self,
        config: OrderedDemuxConfig,
        cancel: CancellationToken,
        worker_factory: WorkerFactory,
    ) -> OrderedDemuxStream<WorkerOutput>
    // heaven forgive me for these types
    where
        Self: Send + 'static,
        Self::Item: Partitioned + Send + 'static,
        <Self::Item as Partitioned>::Key: Send + 'static,
        WorkerOutput: Send + 'static,
        WorkerFactory: Fn(
                <Self::Item as Partitioned>::Key,
                OrderedReceiver<Self::Item>,
                CancellationToken,
            ) -> AwaitableWorkerOutput
            + Send
            + Sync
            + 'static,
        AwaitableWorkerOutput: Future<Output = Result<WorkerOutput>> + Send + 'static;
}

impl<S: Stream + Sized> OrderedDemuxExt for S {
    fn ordered_demux<WorkerOutput, WorkerFactory, AwaitableWorkerOutput>(
        self,
        config: OrderedDemuxConfig,
        cancel: CancellationToken,
        worker_factory: WorkerFactory,
    ) -> OrderedDemuxStream<WorkerOutput>
    where
        Self: Send + 'static,
        Self::Item: Partitioned + Send + 'static,
        <Self::Item as Partitioned>::Key: Send + 'static,
        WorkerOutput: Send + 'static,
        WorkerFactory: Fn(
                <Self::Item as Partitioned>::Key,
                OrderedReceiver<Self::Item>,
                CancellationToken,
            ) -> AwaitableWorkerOutput
            + Send
            + Sync
            + 'static,
        AwaitableWorkerOutput: Future<Output = Result<WorkerOutput>> + Send + 'static,
    {
        OrderedDemuxStream::new(self, config, cancel, worker_factory)
    }
}

/// Output stream from ordered demux.
///
/// Yields results in partition order, wrapping worker outputs in `Result`.
pub struct OrderedDemuxStream<O> {
    inner: Pin<Box<dyn Stream<Item = Result<O>> + Send>>,
    first_error: Arc<FirstError>,
    // need to hold onto this so it is triggered when this stream is dropped
    // but we never use it directly hence the `_`
    _cancel: CancellationToken,
}

impl<WorkerOutput: Send + 'static> OrderedDemuxStream<WorkerOutput> {
    fn new<InputStream, WorkerFactory, AwaitableWorkerOutput>(
        input: InputStream,
        config: OrderedDemuxConfig,
        cancel: CancellationToken,
        worker_factory: WorkerFactory,
    ) -> Self
    where
        InputStream: Stream + Send + 'static,
        InputStream::Item: Partitioned + Send + 'static,
        <InputStream::Item as Partitioned>::Key: Send + 'static,
        WorkerFactory: Fn(
                <InputStream::Item as Partitioned>::Key,
                OrderedReceiver<InputStream::Item>,
                CancellationToken,
            ) -> AwaitableWorkerOutput
            + Send
            + Sync
            + 'static,
        AwaitableWorkerOutput: Future<Output = Result<WorkerOutput>> + Send + 'static,
    {
        let first_error = Arc::new(FirstError::new(cancel.clone()));

        // create ordered channel for output - receiver stays here, sender goes to runner
        let (output_tx, output_rx) =
            ordered_channel::ordered_channel::<Result<WorkerOutput>>(config.output_buffer_size);

        let runner: DemuxRunner<WorkerOutput, WorkerFactory> = DemuxRunner {
            config,
            cancel: cancel.clone(),
            first_error: Arc::clone(&first_error),
            output_tx,
            worker_factory: Arc::new(worker_factory),
        };

        tokio::spawn(runner.run::<InputStream::Item, _, _>(input));

        Self {
            inner: Box::pin(output_rx.into_stream()),
            first_error,
            _cancel: cancel,
        }
    }
}

impl<WorkerOutput> Stream for OrderedDemuxStream<WorkerOutput> {
    type Item = Result<WorkerOutput>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(self.inner.as_mut().poll_next(cx)) {
            Some(result) => Poll::Ready(Some(result)),
            None => {
                if let Some(err) = self.first_error.take() {
                    Poll::Ready(Some(Err(err)))
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}

struct DemuxRunner<WorkerOutput, WorkerFactory> {
    config: OrderedDemuxConfig,
    cancel: CancellationToken,
    first_error: Arc<FirstError>,
    output_tx: OrderedSender<Result<WorkerOutput>>,
    worker_factory: Arc<WorkerFactory>,
}

impl<WorkerOutput, WorkerFactory> DemuxRunner<WorkerOutput, WorkerFactory>
where
    WorkerOutput: Send + 'static,
{
    async fn run<I, S, AwaitableWorkerOutput>(self, input: S)
    where
        I: Partitioned + Send + 'static,
        I::Key: Send + 'static,
        S: Stream<Item = I> + Send + 'static,
        WorkerFactory: Fn(I::Key, OrderedReceiver<I>, CancellationToken) -> AwaitableWorkerOutput
            + Send
            + Sync
            + 'static,
        AwaitableWorkerOutput: Future<Output = Result<WorkerOutput>> + Send + 'static,
    {
        let mut input = std::pin::pin!(input);
        let max_concurrent = self.config.max_concurrent;
        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        let mut partitions: FxHashMap<I::Key, PartitionState<I>> = FxHashMap::default();
        let mut next_output_slot = 0usize;

        // process input items
        loop {
            if self.cancel.is_cancelled() {
                break;
            }

            let item = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => break,
                item = input.next() => item,
            };

            let Some(item) = item else {
                break;
            };

            let key = item.partition_key();
            let seq_idx = item.sequence_index();
            let is_last = item.is_last();

            // get or create partition state
            let partition = match partitions.entry(key.clone()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let permit = Arc::clone(&semaphore)
                        .acquire_owned()
                        .await
                        .expect("semaphore closed");
                    let output_slot = next_output_slot;
                    next_output_slot += 1;

                    let (batch_tx, batch_rx) =
                        ordered_channel::ordered_channel(self.config.worker_buffer_size);

                    let worker_key = entry.key().clone();
                    let worker_factory = Arc::clone(&self.worker_factory);
                    let worker_cancel = self.cancel.clone();
                    let worker_first_error = Arc::clone(&self.first_error);
                    let worker_output_tx = self.output_tx.clone();

                    tokio::spawn(async move {
                        let _permit = permit; // hold until worker completes

                        let result =
                            worker_factory(worker_key, batch_rx, worker_cancel.clone()).await;

                        // send result to output channel
                        if let Err(ordered_channel::SendError::Closed(Err(inner))) =
                            worker_output_tx.send(output_slot, result).await
                            && !worker_cancel.is_cancelled()
                        {
                            worker_first_error.set(inner);
                        }
                    });

                    entry.insert(PartitionState {
                        batch_tx,
                        _output_slot: output_slot,
                    })
                }
            };

            // send item to partition
            if partition.batch_tx.send(seq_idx, item).await.is_err() && !self.cancel.is_cancelled()
            {
                self.first_error
                    .set(anyhow::anyhow!("failed to send batch to partition worker"));
                break;
            }

            if is_last {
                partitions.remove(&key);
            }
        }

        partitions.clear();

        #[allow(clippy::cast_possible_truncation)]
        let _ = semaphore
            .acquire_many(max_concurrent as u32)
            .await
            .expect("semaphore closed");
    }
}

struct PartitionState<I> {
    batch_tx: OrderedSender<I>,
    _output_slot: usize,
}

impl<I: Partitioned> Debug for PartitionState<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PartitionState {{ _output_slot: {} }}",
            self._output_slot
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, sleep};

    #[derive(Debug, Clone)]
    struct TestItem {
        partition: usize,
        seq: usize,
        value: i32,
        is_last: bool,
    }

    impl Partitioned for TestItem {
        type Key = usize;

        fn partition_key(&self) -> Self::Key {
            self.partition
        }

        fn sequence_index(&self) -> usize {
            self.seq
        }

        fn is_last(&self) -> bool {
            self.is_last
        }
    }

    #[tokio::test]
    async fn test_single_partition() {
        let items = vec![
            TestItem {
                partition: 0,
                seq: 0,
                value: 1,
                is_last: false,
            },
            TestItem {
                partition: 0,
                seq: 1,
                value: 2,
                is_last: false,
            },
            TestItem {
                partition: 0,
                seq: 2,
                value: 3,
                is_last: true,
            },
        ];

        let input = stream::iter(items);
        let cancel = CancellationToken::new();
        let config = OrderedDemuxConfig {
            max_concurrent: 2,
            worker_buffer_size: 4,
            output_buffer_size: 4,
        };

        let output = input.ordered_demux(config, cancel, |_key, mut rx, _cancel| async move {
            let mut sum = 0;
            while let Ok(item) = rx.recv().await {
                sum += item.value;
            }
            Ok::<_, anyhow::Error>(sum)
        });

        let results: Vec<_> = output.collect().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap(), &6);
    }

    #[tokio::test]
    async fn test_multiple_partitions() {
        let items = vec![
            TestItem {
                partition: 0,
                seq: 0,
                value: 1,
                is_last: false,
            },
            TestItem {
                partition: 1,
                seq: 0,
                value: 10,
                is_last: false,
            },
            TestItem {
                partition: 0,
                seq: 1,
                value: 2,
                is_last: true,
            },
            TestItem {
                partition: 1,
                seq: 1,
                value: 20,
                is_last: true,
            },
        ];

        let input = stream::iter(items);
        let cancel = CancellationToken::new();
        let config = OrderedDemuxConfig {
            max_concurrent: 2,
            worker_buffer_size: 4,
            output_buffer_size: 4,
        };

        let output = input.ordered_demux(config, cancel, |_key, mut rx, _cancel| async move {
            let mut sum = 0;
            while let Ok(item) = rx.recv().await {
                sum += item.value;
            }
            Ok::<_, anyhow::Error>(sum)
        });

        let results: Vec<_> = output.collect().await;
        assert_eq!(results.len(), 2);
        // partition 0 started first, so its result should come first
        assert_eq!(results[0].as_ref().unwrap(), &3);
        assert_eq!(results[1].as_ref().unwrap(), &30);
    }

    #[tokio::test]
    async fn test_concurrency_limiting() {
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let concurrent_count = Arc::new(AtomicUsize::new(0));

        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let items: Vec<TestItem> = (0..4)
            .map(|i| TestItem {
                partition: i,
                seq: 0,
                value: i as i32,
                is_last: true,
            })
            .collect();

        let input = stream::iter(items);
        let cancel = CancellationToken::new();
        let config = OrderedDemuxConfig {
            max_concurrent: 2,
            worker_buffer_size: 4,
            output_buffer_size: 4,
        };

        let max_concurrent_clone = Arc::clone(&max_concurrent);
        let concurrent_count_clone = Arc::clone(&concurrent_count);

        let output = input.ordered_demux(config, cancel, move |_key, mut rx, _cancel| {
            let max_concurrent = Arc::clone(&max_concurrent_clone);
            let concurrent_count = Arc::clone(&concurrent_count_clone);
            async move {
                // increment concurrent count
                let current = concurrent_count.fetch_add(1, Ordering::SeqCst) + 1;
                max_concurrent.fetch_max(current, Ordering::SeqCst);

                // drain items
                while let Ok(_item) = rx.recv().await {}

                // simulate work
                sleep(Duration::from_millis(10)).await;

                // decrement
                concurrent_count.fetch_sub(1, Ordering::SeqCst);

                Ok::<_, anyhow::Error>(())
            }
        });

        let _results: Vec<_> = output.collect().await;

        // should never exceed max_concurrent of 2
        assert!(max_concurrent.load(Ordering::SeqCst) <= 2);
    }
}
