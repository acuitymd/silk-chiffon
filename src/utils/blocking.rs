//! Utilities for blocking on async code from sync contexts.

use std::future::Future;

/// Block on an async future from a sync context.
///
/// If called from a tokio runtime thread, uses `block_in_place` to yield the
/// worker thread while blocking. Otherwise falls back to a simple block_on.
///
/// # Panics
///
/// Panics if called from a tokio current-thread runtime (`flavor = "current_thread"`).
/// This function requires a multi-threaded runtime when called from within tokio.
pub fn block_on<F: Future>(f: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(f)),
        Err(_) => futures::executor::block_on(f),
    }
}
