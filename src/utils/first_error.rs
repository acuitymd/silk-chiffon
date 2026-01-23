//! Captures the first error and triggers cancellation.
//!
//! Combines a write-once register with a cancellation token so we can
//! shut down pipelines when the first error is encountered and report
//! the correct error that triggered the shutdown.

use std::sync::Mutex;

use anyhow::Error;
use tokio_util::sync::CancellationToken;

/// Captures the first error that occurs and triggers cancellation.
///
/// Thread-safe: multiple workers can call `set()` concurrently.
/// Only the first error is captured; subsequent errors are ignored.
pub struct FirstError {
    error: Mutex<Option<Error>>,
    cancel: CancellationToken,
}

impl Default for FirstError {
    fn default() -> Self {
        Self::new()
    }
}

impl FirstError {
    /// Create a new FirstError that will trigger the given cancellation signal.
    pub fn new() -> Self {
        Self {
            error: Mutex::new(None),
            cancel: CancellationToken::new(),
        }
    }

    /// Set an error, triggering cancellation.
    ///
    /// Returns `true` if this was the first error, `false` if an error was already set
    /// or cancellation was already triggered.
    pub fn set(&self, err: Error) -> bool {
        if self.is_cancelled() {
            return false;
        }
        let mut guard = self.error.lock().unwrap_or_else(|e| e.into_inner());
        if self.is_cancelled() {
            return false;
        }
        if guard.is_none() {
            *guard = Some(err);
            self.cancel.cancel();
            true
        } else {
            false
        }
    }

    /// Take the captured error, if any.
    ///
    /// Returns `None` if cancellation was triggered and error was already taken.
    /// Check `is_cancelled()` to see if the error was previously set.
    pub fn take(&self) -> Option<Error> {
        self.error.lock().unwrap_or_else(|e| e.into_inner()).take()
    }

    /// Check if cancellation was triggered.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Get a clone of the cancellation token.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn test_first_error_captures_first() {
        let first_error = FirstError::new();
        let cancel = first_error.cancel_token();

        assert!(!first_error.is_cancelled());
        assert!(!cancel.is_cancelled());

        // first error is captured
        assert!(first_error.set(anyhow!("error 1")));
        assert!(first_error.is_cancelled());
        assert!(cancel.is_cancelled());

        // second error is ignored
        assert!(!first_error.set(anyhow!("error 2")));

        // take returns first error
        let err = first_error.take().unwrap();
        assert_eq!(err.to_string(), "error 1");

        // take returns None after taking
        assert!(first_error.take().is_none());
    }

    #[test]
    fn test_first_error_no_error() {
        let first_error = FirstError::new();
        let cancel = first_error.cancel_token();

        assert!(!first_error.is_cancelled());
        assert!(first_error.take().is_none());
        assert!(!cancel.is_cancelled());
    }

    #[test]
    fn test_first_error_rejects_after_take() {
        let first_error = FirstError::new();

        // set and take
        assert!(first_error.set(anyhow!("error 1")));
        let _ = first_error.take();

        // slot is empty but cancellation was triggered, so new errors rejected
        assert!(!first_error.set(anyhow!("error 2")));
        assert!(first_error.take().is_none());
    }
}
