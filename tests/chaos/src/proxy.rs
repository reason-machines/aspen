//! A fault-injecting TCP proxy.
//!
//! The suite puts one in front of Postgres (Aspen ↔ database) and one in
//! front of the Aspen instances (clients ↔ Aspen, where it also spreads
//! connections over the live instances like a load balancer). Each can,
//! at any moment:
//!
//! - **partition**: stop moving bytes in both directions on every
//!   connection, old and new, until healed. Nothing is closed, so this is
//!   what a dropped-packet partition looks like to both ends: TCP waits,
//!   and resumes when the partition heals;
//! - **close**: close every open connection at once (with `shutdown`, so
//!   each peer sees an orderly FIN, not an RST);
//! - **refuse**: close new connections as soon as they are accepted;
//! - **delay**: hold every chunk of data for a fixed time before passing it
//!   on;
//! - **retarget**: send new connections to different upstreams (a
//!   database failover moves the proxy to the promoted standby).

use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

/// How the proxy treats traffic right now.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Faults {
    partitioned: bool,
    refuse: bool,
    delay: Duration,
}

#[derive(Debug)]
struct Conn {
    id: u64,
    client: TcpStream,
    upstream: TcpStream,
}

#[derive(Debug, Default)]
struct Shared {
    upstreams: Mutex<Vec<SocketAddr>>,
    next: AtomicUsize,
    faults: Mutex<Faults>,
    changed: Condvar,
    conns: Mutex<Vec<Conn>>,
    /// Bumped by every close; a connection opened before a close and still
    /// waiting to forward sees it and gives up.
    closes: AtomicU64,
    stop: AtomicBool,
    accepted: AtomicU64,
    ids: AtomicU64,
}

/// A TCP proxy on an ephemeral port of 127.0.0.1. Stops accepting, and
/// closes every connection, when dropped.
#[derive(Debug)]
pub struct TcpProxy {
    addr: SocketAddr,
    name: String,
    shared: Arc<Shared>,
}

impl TcpProxy {
    /// Starts a proxy that forwards to `upstreams`, round robin over the
    /// ones that accept a connection.
    pub fn start(name: &str, upstreams: Vec<SocketAddr>) -> io::Result<Self> {
        Self::start_on(
            name,
            "127.0.0.1:0".parse().expect("valid address"),
            upstreams,
        )
    }

    /// Like [`TcpProxy::start`], on a given address.
    pub fn start_on(name: &str, at: SocketAddr, upstreams: Vec<SocketAddr>) -> io::Result<Self> {
        let listener = TcpListener::bind(at)?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let shared = Arc::new(Shared {
            upstreams: Mutex::new(upstreams),
            ..Shared::default()
        });
        let s = Arc::clone(&shared);
        thread::Builder::new()
            .name(format!("proxy-{name}"))
            .spawn(move || accept_loop(&listener, &s))?;
        Ok(Self {
            addr,
            name: name.to_owned(),
            shared,
        })
    }

    /// The address clients connect to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The label used in reports.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Connections accepted so far.
    pub fn accepted(&self) -> u64 {
        self.shared.accepted.load(Ordering::Relaxed)
    }

    /// Sends new connections to `upstreams` from now on. Open connections
    /// keep their upstream; [`TcpProxy::close_all`] closes them.
    pub fn retarget(&self, upstreams: Vec<SocketAddr>) {
        *lock(&self.shared.upstreams) = upstreams;
    }

    /// Starts or heals a partition.
    pub fn partition(&self, on: bool) {
        self.update(|f| f.partitioned = on);
    }

    /// Refuses (closes at once) new connections, or stops refusing.
    pub fn refuse(&self, on: bool) {
        self.update(|f| f.refuse = on);
    }

    /// Holds each chunk of data for `delay` before forwarding it.
    pub fn delay(&self, delay: Duration) {
        self.update(|f| f.delay = delay);
    }

    /// Closes every open connection.
    pub fn close_all(&self) {
        self.shared.closes.fetch_add(1, Ordering::SeqCst);
        for c in lock(&self.shared.conns).iter() {
            let _ = c.client.shutdown(Shutdown::Both);
            let _ = c.upstream.shutdown(Shutdown::Both);
        }
        self.shared.changed.notify_all();
    }

    /// Clears every fault: no partition, no refusal, no delay.
    pub fn heal(&self) {
        self.update(|f| *f = Faults::default());
    }

    fn update(&self, f: impl FnOnce(&mut Faults)) {
        f(&mut lock(&self.shared.faults));
        self.shared.changed.notify_all();
    }
}

impl Drop for TcpProxy {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::SeqCst);
        self.heal();
        self.close_all();
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn accept_loop(listener: &TcpListener, shared: &Arc<Shared>) {
    while !shared.stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((client, _)) => {
                shared.accepted.fetch_add(1, Ordering::Relaxed);
                let s = Arc::clone(shared);
                let _ = thread::Builder::new()
                    .name("proxy-conn".into())
                    .spawn(move || serve(client, &s));
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(_) => thread::sleep(Duration::from_millis(20)),
        }
    }
}

/// Waits while the proxy is partitioned. Returns false when the
/// connection should be dropped instead (the proxy stopped, or a close
/// happened since `epoch`).
fn pass(shared: &Shared, epoch: u64) -> Option<Duration> {
    let mut faults = lock(&shared.faults);
    loop {
        if shared.stop.load(Ordering::SeqCst) || shared.closes.load(Ordering::SeqCst) != epoch {
            return None;
        }
        if !faults.partitioned {
            return Some(faults.delay);
        }
        faults = shared
            .changed
            .wait_timeout(faults, Duration::from_millis(200))
            .unwrap_or_else(|e| e.into_inner())
            .0;
    }
}

fn serve(client: TcpStream, shared: &Arc<Shared>) {
    let _ = client.set_nonblocking(false);
    let _ = client.set_nodelay(true);
    let epoch = shared.closes.load(Ordering::SeqCst);
    if lock(&shared.faults).refuse {
        let _ = client.shutdown(Shutdown::Both);
        return;
    }
    // A partition stops connection attempts too: hold the client until it
    // heals (or a close), then connect.
    if pass(shared, epoch).is_none() {
        let _ = client.shutdown(Shutdown::Both);
        return;
    }
    let Some(upstream) = connect(shared) else {
        let _ = client.shutdown(Shutdown::Both);
        return;
    };
    let _ = upstream.set_nodelay(true);
    let id = shared.ids.fetch_add(1, Ordering::Relaxed);
    let (Ok(c2), Ok(u2)) = (client.try_clone(), upstream.try_clone()) else {
        return;
    };
    lock(&shared.conns).push(Conn {
        id,
        client: c2,
        upstream: u2,
    });
    // A close that ran between `pass` and the registration missed this
    // connection; honour it now.
    if shared.closes.load(Ordering::SeqCst) != epoch {
        let _ = client.shutdown(Shutdown::Both);
        let _ = upstream.shutdown(Shutdown::Both);
    }
    let (Ok(c3), Ok(u3)) = (client.try_clone(), upstream.try_clone()) else {
        return;
    };
    let s = Arc::clone(shared);
    let up = thread::spawn(move || pump(c3, u3, &s, epoch));
    pump(upstream, client, shared, epoch);
    let _ = up.join();
    lock(&shared.conns).retain(|c| c.id != id);
}

/// Connects to the next upstream that accepts, round robin.
fn connect(shared: &Shared) -> Option<TcpStream> {
    let upstreams = lock(&shared.upstreams).clone();
    if upstreams.is_empty() {
        return None;
    }
    let start = shared.next.fetch_add(1, Ordering::Relaxed);
    (0..upstreams.len()).find_map(|i| {
        let addr = upstreams[(start + i) % upstreams.len()];
        TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()
    })
}

/// Copies `from` to `to` until EOF or an error, honouring the faults.
fn pump(mut from: TcpStream, mut to: TcpStream, shared: &Shared, epoch: u64) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match from.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let Some(delay) = pass(shared, epoch) else {
            break;
        };
        if !delay.is_zero() {
            thread::sleep(delay);
        }
        if to.write_all(&buf[..n]).is_err() {
            break;
        }
    }
    // Half-close: the other direction may still be answering.
    let _ = to.shutdown(Shutdown::Write);
    if shared.closes.load(Ordering::SeqCst) != epoch {
        let _ = from.shutdown(Shutdown::Both);
        let _ = to.shutdown(Shutdown::Both);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    /// An upstream that echoes every byte back.
    fn echo() -> SocketAddr {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        thread::spawn(move || {
            for s in l.incoming() {
                let Ok(mut s) = s else { continue };
                thread::spawn(move || {
                    let mut r = s.try_clone().unwrap();
                    let _ = io::copy(&mut r, &mut s);
                });
            }
        });
        addr
    }

    fn roundtrip(p: &TcpProxy, timeout: Duration) -> io::Result<Vec<u8>> {
        let mut s = TcpStream::connect(p.addr())?;
        s.set_read_timeout(Some(timeout))?;
        s.write_all(b"ping")?;
        let mut got = [0u8; 4];
        s.read_exact(&mut got)?;
        Ok(got.to_vec())
    }

    #[test]
    fn forwards_and_heals_after_a_partition() {
        let p = TcpProxy::start("t", vec![echo()]).unwrap();
        assert_eq!(roundtrip(&p, Duration::from_secs(5)).unwrap(), b"ping");
        p.partition(true);
        assert!(
            roundtrip(&p, Duration::from_millis(300)).is_err(),
            "partitioned"
        );
        // A connection opened during the partition resumes once it heals.
        let mut s = TcpStream::connect(p.addr()).unwrap();
        s.write_all(b"late").unwrap();
        thread::sleep(Duration::from_millis(200));
        p.heal();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut got = [0u8; 4];
        s.read_exact(&mut got).unwrap();
        assert_eq!(&got, b"late");
    }

    #[test]
    fn close_all_closes_open_connections() {
        let p = TcpProxy::start("t", vec![echo()]).unwrap();
        let mut s = TcpStream::connect(p.addr()).unwrap();
        s.write_all(b"ping").unwrap();
        let mut got = [0u8; 4];
        s.read_exact(&mut got).unwrap();
        p.close_all();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let _ = s.write_all(b"more");
        let mut rest = Vec::new();
        let n = s.read_to_end(&mut rest).unwrap_or(0);
        assert_eq!(n, 0, "closed by the close");
    }

    #[test]
    fn refuses_delays_and_retargets() {
        let p = TcpProxy::start("t", vec![echo()]).unwrap();
        p.refuse(true);
        assert!(roundtrip(&p, Duration::from_secs(2)).is_err());
        p.heal();
        p.delay(Duration::from_millis(150));
        let t = Instant::now();
        roundtrip(&p, Duration::from_secs(5)).unwrap();
        assert!(
            t.elapsed() >= Duration::from_millis(300),
            "both directions held"
        );
        p.heal();
        // The first upstream is gone; the proxy skips to one that answers.
        let dead = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        p.retarget(vec![dead, echo()]);
        for _ in 0..3 {
            roundtrip(&p, Duration::from_secs(5)).unwrap();
        }
    }
}
