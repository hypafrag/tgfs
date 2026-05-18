//! Progress-bar styling and `AsyncRead` adapters used by upload paths.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};
use tokio::io::{AsyncRead, ReadBuf};

// Width of the label column that precedes the `[bar ...]` block. Used by
// every progress style below so the file and total bars line up vertically.
pub const LABEL_WIDTH: usize = 48;

pub struct ProgressReader<R> {
    pub inner: R,
    pub file_pb: ProgressBar,
    pub total_pb: ProgressBar,
}

impl<R: AsyncRead + Unpin> AsyncRead for ProgressReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let r = Pin::new(&mut self.inner).poll_read(cx, buf);
        let after = buf.filled().len();
        let delta = (after - before) as u64;
        if delta > 0 {
            self.file_pb.inc(delta);
            self.total_pb.inc(delta);
        }
        r
    }
}

/// AsyncRead view over an `Arc<Vec<u8>>` slice. Used so multiple parts that
/// reference different ranges of the same encoded buffer can stream
/// independently without copying.
pub struct SliceReader {
    pub buf: Arc<Vec<u8>>,
    pub start: usize,
    pub end: usize,
    pub pos: usize,
}

impl AsyncRead for SliceReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let _ = self.start;
        let remaining = self.end.saturating_sub(self.pos);
        if remaining == 0 { return Poll::Ready(Ok(())); }
        let n = std::cmp::min(remaining, out.remaining());
        out.put_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Poll::Ready(Ok(()))
    }
}

/// Middle-truncate `s` to at most `max` characters with a `…` in the gap.
/// indicatif's `{msg:<W.W}` doesn't truncate (it only left-pads); we shorten
/// labels manually so the `[bar …]` column stays aligned regardless of how
/// long the source filename is.
fn truncate_middle(s: &str, max: usize) -> String {
    let len = s.chars().count();
    if len <= max { return s.to_string(); }
    if max <= 1 { return "…".to_string(); }
    let keep = max - 1; // 1 char for the ellipsis itself
    let head = keep.div_ceil(2);
    let tail = keep - head;
    let chars: Vec<char> = s.chars().collect();
    let mut out: String = chars[..head].iter().collect();
    out.push('…');
    out.extend(chars[chars.len() - tail..].iter());
    out
}

pub fn set_label(pb: &ProgressBar, label: impl Into<String>) {
    pb.set_message(truncate_middle(&label.into(), LABEL_WIDTH));
}

pub fn set_bar_style(pb: &ProgressBar) {
    pb.disable_steady_tick();
    pb.set_style(
        ProgressStyle::with_template(&format!(
            "{{msg:<{w}.{w}}} [{{bar:30.cyan/blue}}] {{bytes}}/{{total_bytes}} ({{bytes_per_sec}}, {{eta}})",
            w = LABEL_WIDTH,
        ))
        .unwrap()
        .progress_chars("=>-"),
    );
}

pub fn set_spinner_style(pb: &ProgressBar) {
    pb.set_style(
        ProgressStyle::with_template(&format!(
            "{{msg:<{w}.{w}}} {{spinner:.cyan}} {{bytes}} ({{bytes_per_sec}})",
            w = LABEL_WIDTH,
        ))
        .unwrap(),
    );
    pb.enable_steady_tick(Duration::from_millis(120));
}
