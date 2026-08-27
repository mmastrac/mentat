//! Tiny dependency-free HTTP side-port: /metrics (Prometheus text), /status
//! (JSON), /healthz, and /events (WebSocket event stream with a snapshot on
//! connect). The daemon's message rate makes a real HTTP stack pointless;
//! hand-rolling keeps the binary free of an async runtime.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::time::Duration;

use serde_json::json;

use crate::logfmt::log;
use crate::state::{ActorState, SharedRef};

pub fn serve(shared: SharedRef, port: u16) {
    std::thread::spawn(move || {
        let listener = match TcpListener::bind(("0.0.0.0", port)) {
            Ok(l) => l,
            Err(e) => {
                log(
                    "http_bind_failed",
                    &[("port", port.to_string()), ("error", e.to_string())],
                );
                return;
            }
        };
        log("http_up", &[("port", port.to_string())]);
        for stream in listener.incoming().flatten() {
            let shared = shared.clone();
            std::thread::spawn(move || handle(shared, stream));
        }
    });
}

fn handle(shared: SharedRef, mut stream: TcpStream) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
    let Some((path, headers)) = read_request(&mut stream) else {
        return;
    };
    let path_only = path.split('?').next().unwrap_or("");
    let query = path.split('?').nth(1).unwrap_or("");

    match path_only {
        "/healthz" => respond(&mut stream, 200, "text/plain", b"ok\n"),
        "/metrics" => {
            let body = metrics(&shared);
            respond(&mut stream, 200, "text/plain; version=0.0.4", body.as_bytes())
        }
        "/status" => {
            let group = query
                .split('&')
                .find_map(|kv| kv.strip_prefix("group="))
                .map(|s| s.to_string());
            let st = shared.st.lock().unwrap();
            let snap = crate::status::snapshot(&st, group.as_deref());
            drop(st);
            respond(&mut stream, 200, "application/json", snap.to_string().as_bytes())
        }
        "/events" => {
            if headers
                .get("upgrade")
                .map(|v| v.to_ascii_lowercase().contains("websocket"))
                .unwrap_or(false)
            {
                websocket_events(shared, stream, &headers);
            } else {
                respond(
                    &mut stream,
                    400,
                    "text/plain",
                    b"/events is a WebSocket endpoint\n",
                )
            }
        }
        _ => respond(&mut stream, 404, "text/plain", b"not found\n"),
    }
}

fn read_request(stream: &mut TcpStream) -> Option<(String, HashMap<String, String>)> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        if buf.len() > 16 * 1024 {
            return None;
        }
        match stream.read(&mut byte) {
            Ok(1) => buf.push(byte[0]),
            _ => return None,
        }
    }
    let text = String::from_utf8_lossy(&buf);
    let mut lines = text.lines();
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?;
    if method != "GET" {
        return None;
    }
    let path = parts.next()?.to_string();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    Some((path, headers))
}

fn respond(stream: &mut TcpStream, code: u16, ctype: &str, body: &[u8]) {
    let reason = match code {
        200 => "OK",
        400 => "Bad Request",
        _ => "Not Found",
    };
    let head = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
}

fn metrics(shared: &SharedRef) -> String {
    let st = shared.st.lock().unwrap();
    let mut out = String::new();
    out.push_str("# TYPE mentat_build_info gauge\n");
    out.push_str(&format!(
        "mentat_build_info{{version=\"{}\"}} 1\n",
        env!("CARGO_PKG_VERSION")
    ));

    let mut groups: Vec<String> = st.agents.values().map(|a| a.group.clone()).collect();
    groups.sort();
    groups.dedup();

    out.push_str("# TYPE mentat_agents gauge\n# TYPE mentat_gpus_total gauge\n# TYPE mentat_gpus_used gauge\n");
    for g in &groups {
        let alive = st.agents.values().filter(|a| a.alive && &a.group == g);
        let mut n = 0usize;
        let mut total = 0usize;
        let mut free = 0usize;
        let mut vendor = String::from("nvidia");
        for a in alive {
            n += 1;
            total += a.gpus.len();
            free += st.free_gpus_of(&a.id).len();
            vendor = a.gpu_vendor.clone();
        }
        out.push_str(&format!(
            "mentat_agents{{group=\"{g}\",vendor=\"{vendor}\"}} {n}\n"
        ));
        out.push_str(&format!("mentat_gpus_total{{group=\"{g}\"}} {total}\n"));
        out.push_str(&format!(
            "mentat_gpus_used{{group=\"{g}\"}} {}\n",
            total - free
        ));
    }

    out.push_str("# TYPE mentat_actors gauge\n");
    for g in &groups {
        let mut spawning = 0;
        let mut running = 0;
        let mut dead = 0;
        for a in st.actors.values().filter(|a| &a.group == g) {
            match a.state {
                ActorState::Spawning => spawning += 1,
                ActorState::Running => running += 1,
                ActorState::Dead { .. } => dead += 1,
            }
        }
        for (state, v) in [("spawning", spawning), ("running", running), ("dead", dead)] {
            out.push_str(&format!(
                "mentat_actors{{group=\"{g}\",state=\"{state}\"}} {v}\n"
            ));
        }
    }

    out.push_str("# TYPE mentat_actors_spawned_total counter\n");
    out.push_str(&format!(
        "mentat_actors_spawned_total {}\n",
        st.counters.actors_spawned
    ));
    out.push_str("# TYPE mentat_actor_exits_total counter\n");
    for (kind, v) in [
        ("clean", st.counters.actor_exits_clean),
        ("signal", st.counters.actor_exits_signal),
        ("error", st.counters.actor_exits_error),
    ] {
        out.push_str(&format!("mentat_actor_exits_total{{kind=\"{kind}\"}} {v}\n"));
    }
    out.push_str("# TYPE mentat_calls_total counter\n");
    out.push_str(&format!("mentat_calls_total {}\n", st.counters.calls_total));
    out.push_str("# TYPE mentat_clients_total counter\n");
    out.push_str(&format!(
        "mentat_clients_total {}\n",
        st.counters.clients_total
    ));
    out.push_str("# TYPE mentat_agents_registered_total counter\n");
    out.push_str(&format!(
        "mentat_agents_registered_total {}\n",
        st.counters.agents_registered
    ));
    out.push_str("# TYPE mentat_event_subscribers gauge\n");
    out.push_str(&format!(
        "mentat_event_subscribers {}\n",
        st.event_subs.len()
    ));
    out
}

// ---------------------------------------------------------------------------
// WebSocket: server-side handshake plus outbound text frames. Inbound frames
// are drained and discarded (a subscriber has nothing to say to us); close is
// detected by EOF on the drain thread or a failed write.
// ---------------------------------------------------------------------------

fn websocket_events(shared: SharedRef, mut stream: TcpStream, headers: &HashMap<String, String>) {
    let Some(key) = headers.get("sec-websocket-key") else {
        return;
    };
    let accept = base64(&sha1(format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes()));
    let resp = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    if stream.write_all(resp.as_bytes()).is_err() {
        return;
    }
    let _ = stream.set_read_timeout(None);

    // Snapshot first, then live events, registered under the same lock so no
    // event can fall between them.
    let (tx, rx) = mpsc::channel::<String>();
    let first = {
        let mut st = shared.st.lock().unwrap();
        let snap = crate::status::snapshot(&st, None);
        st.event_subs.push(tx);
        json!({ "type": "snapshot", "seq": st.next_event_seq - 1, "data": snap }).to_string()
    };
    log("ws_subscribe", &[]);

    // Drain inbound bytes; EOF shuts the socket so the send loop notices.
    {
        let stream = match stream.try_clone() {
            Ok(s) => s,
            Err(_) => return,
        };
        std::thread::spawn(move || {
            let mut s = stream;
            let mut buf = [0u8; 1024];
            loop {
                match s.read(&mut buf) {
                    Ok(0) | Err(_) => {
                        let _ = s.shutdown(std::net::Shutdown::Both);
                        return;
                    }
                    Ok(_) => {}
                }
            }
        });
    }

    if ws_send_text(&mut stream, &first).is_err() {
        return;
    }
    loop {
        match rx.recv_timeout(Duration::from_secs(15)) {
            Ok(line) => {
                if ws_send_text(&mut stream, &line).is_err() {
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // Ping frame keeps intermediaries open and detects dead peers.
                if stream.write_all(&[0x89, 0x00]).is_err() {
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    log("ws_unsubscribe", &[]);
    // Dropping rx makes the daemon's next emit() prune our sender.
}

fn ws_send_text(stream: &mut TcpStream, text: &str) -> std::io::Result<()> {
    let bytes = text.as_bytes();
    let mut frame: Vec<u8> = vec![0x81]; // FIN + text opcode
    match bytes.len() {
        n if n < 126 => frame.push(n as u8),
        n if n <= 0xFFFF => {
            frame.push(126);
            frame.extend_from_slice(&(n as u16).to_be_bytes());
        }
        n => {
            frame.push(127);
            frame.extend_from_slice(&(n as u64).to_be_bytes());
        }
    }
    frame.extend_from_slice(bytes);
    stream.write_all(&frame)
}

/// SHA-1, needed only for the WebSocket handshake (RFC 6455 mandates it).
fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];
    let ml = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&ml.to_be_bytes());

    for chunk in msg.chunks(64) {
        let mut w = [0u32; 80];
        for (i, word) in chunk.chunks(4).enumerate() {
            w[i] = u32::from_be_bytes(word.try_into().unwrap());
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h[0], h[1], h[2], h[3], h[4]);
        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let tmp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = tmp;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }
    let mut out = [0u8; 20];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

fn base64(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | (b[2] as u32);
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc6455_handshake_vector() {
        // The example from RFC 6455 section 1.3.
        let accept = base64(&sha1(
            b"dGhlIHNhbXBsZSBub25jZQ==258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
        ));
        assert_eq!(accept, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }
}
