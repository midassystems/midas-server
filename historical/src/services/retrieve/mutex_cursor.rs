use std::io::Cursor;
use std::io::{self};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio::sync::Mutex;

pub struct MutexCursor {
    inner: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl MutexCursor {
    pub fn new(cursor: Arc<Mutex<Cursor<Vec<u8>>>>) -> Self {
        Self { inner: cursor }
    }
}

impl AsyncWrite for MutexCursor {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.inner.try_lock() {
            Ok(mut cursor) => Poll::Ready(std::io::Write::write(&mut *cursor, buf)), // Lock acquired successfully
            Err(_) => {
                // If the lock is not available, return Pending
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.inner.try_lock() {
            Ok(mut cursor) => {
                // Explicitly use the `std::io::Write` trait for `flush`
                Poll::Ready(std::io::Write::flush(&mut *cursor))
            } // Lock acquired successfully
            Err(_) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // No additional shutdown behavior needed for Cursor
        Poll::Ready(Ok(()))
    }
}
