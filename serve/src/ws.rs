//! Minimal WebSocket client for mentatd's /events stream. Read-only:
//! nothing is sent after the handshake, so the RFC's client-side masking
//! rule never applies. The server side is in mentat's http.rs.

use std::io;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A frame larger than this is a corrupt length prefix.
const MAX_FRAME: u64 = 64 * 1024 * 1024;

pub struct EventStream {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl EventStream {
    pub async fn connect(addr: &str) -> io::Result<EventStream> {
        let mut stream = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timeout"))??;
        // Fixed key (RFC 6455's own sample nonce). The key defeats caching
        // intermediaries rather than authenticating, and the daemon does not
        // check randomness, so a fixed value saves a rand dependency.
        let req = format!(
            "GET /events HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\n\
             Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Sec-WebSocket-Version: 13\r\n\r\n"
        );
        stream.write_all(req.as_bytes()).await?;

        let mut buf: Vec<u8> = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(pos) = find(&buf, b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..pos]).into_owned();
                let status_line = head.lines().next().unwrap_or("");
                if !status_line.contains(" 101") {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("handshake refused: {status_line}"),
                    ));
                }
                let rest = buf[pos + 4..].to_vec();
                return Ok(EventStream { stream, buf: rest });
            }
            if buf.len() > 64 * 1024 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "handshake response over 64 KiB",
                ));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "handshake timeout"));
            }
            let mut tmp = [0u8; 4096];
            let n = tokio::time::timeout(remaining, stream.read(&mut tmp))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "handshake timeout"))??;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF in handshake",
                ));
            }
            buf.extend_from_slice(&tmp[..n]);
        }
    }

    /// Next text frame, or Ok(None) once `tick` elapses without one -- the
    /// caller uses the tick as its periodic re-poll interval. Err on EOF,
    /// a close frame, or protocol garbage. Ping/pong frames are swallowed.
    pub async fn next(&mut self, tick: Duration) -> io::Result<Option<String>> {
        let deadline = Instant::now() + tick;
        loop {
            while let Some((op, payload)) = self.try_parse()? {
                match op {
                    0x1 => return Ok(Some(String::from_utf8_lossy(&payload).into_owned())),
                    0x8 => {
                        return Err(io::Error::new(
                            io::ErrorKind::ConnectionAborted,
                            "daemon closed the websocket",
                        ))
                    }
                    _ => {}
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            let mut tmp = [0u8; 65536];
            match tokio::time::timeout(remaining, self.stream.read(&mut tmp)).await {
                Err(_) => return Ok(None),
                Ok(Ok(0)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "websocket EOF",
                    ))
                }
                Ok(Ok(n)) => self.buf.extend_from_slice(&tmp[..n]),
                Ok(Err(e)) => return Err(e),
            }
        }
    }

    /// One complete frame off the buffer, or None until more bytes arrive.
    fn try_parse(&mut self) -> io::Result<Option<(u8, Vec<u8>)>> {
        let b = &self.buf;
        if b.len() < 2 {
            return Ok(None);
        }
        let op = b[0] & 0x0F;
        if b[1] & 0x80 != 0 {
            // Servers must not mask (RFC 6455 5.1); ours never does.
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "masked server frame",
            ));
        }
        let mut len = (b[1] & 0x7F) as u64;
        let mut off = 2usize;
        if len == 126 {
            if b.len() < 4 {
                return Ok(None);
            }
            len = u16::from_be_bytes(b[2..4].try_into().unwrap()) as u64;
            off = 4;
        } else if len == 127 {
            if b.len() < 10 {
                return Ok(None);
            }
            len = u64::from_be_bytes(b[2..10].try_into().unwrap());
            off = 10;
        }
        if len > MAX_FRAME {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("frame of {len} bytes"),
            ));
        }
        let total = off + len as usize;
        if b.len() < total {
            return Ok(None);
        }
        let payload = b[off..total].to_vec();
        self.buf.drain(..total);
        Ok(Some((op, payload)))
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}
