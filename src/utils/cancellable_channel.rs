//! Cancellable channel wrappers built on tokio mpsc.
//!
//! Provides wrappers around tokio bounded channels that support graceful
//! cancellation via a shared CancellationToken.

use std::fmt::{self, Display, Formatter};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

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
    tx: mpsc::Sender<T>,
    token: CancellationToken,
    channel_name: &'static str,
    task_name: Option<String>,
}

impl<T> CancellableSender<T> {
    pub fn new(tx: mpsc::Sender<T>, token: CancellationToken, channel_name: &'static str) -> Self {
        Self {
            tx,
            token,
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
            _ = self.token.cancelled() => Err(ChannelError::Cancelled),
            result = self.tx.send(value) => {
                result.map_err(|_| ChannelError::Disconnected {
                    channel: self.channel_name,
                    task: self.task_name.clone(),
                })
            }
        }
    }
}

impl<T> Clone for CancellableSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            token: self.token.clone(),
            channel_name: self.channel_name,
            task_name: None,
        }
    }
}

pub struct CancellableReceiver<T> {
    rx: mpsc::Receiver<T>,
    token: CancellationToken,
    channel_name: &'static str,
    task_name: Option<String>,
}

impl<T> CancellableReceiver<T> {
    pub fn new(
        rx: mpsc::Receiver<T>,
        token: CancellationToken,
        channel_name: &'static str,
    ) -> Self {
        Self {
            rx,
            token,
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
            _ = self.token.cancelled() => Err(ChannelError::Cancelled),
            msg = self.rx.recv() => {
                msg.ok_or(ChannelError::Disconnected {
                    channel: self.channel_name,
                    task: self.task_name.clone(),
                })
            }
        }
    }
}

/// Create a cancellable bounded channel.
pub fn cancellable_channel<T>(
    capacity: usize,
    token: CancellationToken,
    channel_name: &'static str,
) -> (CancellableSender<T>, CancellableReceiver<T>) {
    let (tx, rx) = mpsc::channel(capacity);
    (
        CancellableSender::new(tx, token.clone(), channel_name),
        CancellableReceiver::new(rx, token, channel_name),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_send_recv_basic() {
        let token = CancellationToken::new();
        let (tx, mut rx) = cancellable_channel(4, token, "test");

        tx.send(42).await.unwrap();
        let received = rx.recv().await.unwrap();
        assert_eq!(received, 42);
    }

    #[tokio::test]
    async fn test_cancelled_send() {
        let token = CancellationToken::new();
        let (tx, mut _rx) = cancellable_channel::<i32>(1, token.clone(), "test");

        token.cancel();

        let result = tx.send(42).await;
        assert!(matches!(result, Err(ChannelError::Cancelled)));
    }

    #[tokio::test]
    async fn test_cancelled_recv() {
        let token = CancellationToken::new();
        let (_tx, mut rx) = cancellable_channel::<i32>(4, token.clone(), "test");

        token.cancel();

        let result = rx.recv().await;
        assert!(matches!(result, Err(ChannelError::Cancelled)));
    }

    #[tokio::test]
    async fn test_disconnected_on_drop() {
        let token = CancellationToken::new();
        let (tx, mut rx) = cancellable_channel::<i32>(4, token, "test");

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
        let token = CancellationToken::new();
        let (tx, rx) = cancellable_channel::<i32>(4, token, "input");
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
        let token = CancellationToken::new();
        let (tx, rx) = cancellable_channel::<i32>(4, token, "output");
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
}
