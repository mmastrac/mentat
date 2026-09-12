//! Binding the announcement port so a daemon and a router can share it.
//!
//! Both run with host networking, and on a box that hosts a model and the
//! router they land on the same port. A plain bind gives the second one
//! `EADDRINUSE`. The two are not in conflict: each wants its own copy of the
//! same broadcasts.

use std::io;
use std::net::UdpSocket;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

/// A UDP socket on `0.0.0.0:port` that other programs may also bind.
///
/// Every listener gets a copy of a broadcast datagram, which is how
/// announcements arrive in a deployment. A unicast datagram reaches one of
/// them, so `MENTAT_ANNOUNCE_ADDR` targets a port with one listener on it.
pub fn bind_shared(port: u16) -> io::Result<UdpSocket> {
    // SAFETY: the fd is owned from creation and closed by OwnedFd on any
    // early return, so nothing else can observe it half-configured.
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let owned = OwnedFd::from_raw_fd(fd);
        // REUSEADDR alone lets a second program bind on Linux. The BSDs,
        // macOS among them, want REUSEPORT for the same effect.
        for opt in [libc::SO_REUSEADDR, libc::SO_REUSEPORT] {
            let on: libc::c_int = 1;
            if libc::setsockopt(
                owned.as_raw_fd(),
                libc::SOL_SOCKET,
                opt,
                &on as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            ) < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        let addr = libc::sockaddr_in {
            #[cfg(any(target_os = "macos", target_os = "ios"))]
            sin_len: std::mem::size_of::<libc::sockaddr_in>() as u8,
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: port.to_be(),
            sin_addr: libc::in_addr {
                s_addr: libc::INADDR_ANY.to_be(),
            },
            sin_zero: [0; 8],
        };
        if libc::bind(
            owned.as_raw_fd(),
            &addr as *const _ as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        ) < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(UdpSocket::from(owned))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The case a co-located daemon and router hit.
    #[test]
    fn two_sockets_share_one_port() {
        let a = bind_shared(0).unwrap();
        let port = a.local_addr().unwrap().port();
        let b = bind_shared(port).expect("second bind on the same port");
        assert_eq!(b.local_addr().unwrap().port(), port);
    }
}
