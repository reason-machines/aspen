//! Running child processes with a deadline.
//!
//! Under injected faults a `git` client can wait on a stalled connection
//! for a long time, so every command the suite runs has a hard timeout. A
//! command that hits it is killed and reported as timed out, which the
//! workload treats as an operation with an unknown outcome.

use std::io::{self, Read};
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// What a command left behind.
#[derive(Debug)]
pub struct Finished {
    /// The exit status, or `None` when the command was killed at its
    /// deadline.
    pub status: Option<ExitStatus>,
    /// Everything it wrote to stdout.
    pub stdout: String,
    /// Everything it wrote to stderr.
    pub stderr: String,
}

impl Finished {
    /// Whether the command exited with status 0.
    pub fn success(&self) -> bool {
        self.status.is_some_and(|s| s.success())
    }

    /// The exit code, or `None` when it was killed or died from a signal.
    pub fn code(&self) -> Option<i32> {
        self.status.and_then(|s| s.code())
    }

    /// stdout and stderr, for error messages.
    pub fn describe(&self) -> String {
        let status = match self.status {
            Some(s) => s.to_string(),
            None => "killed at its deadline".to_owned(),
        };
        format!(
            "{status}\n--- stdout\n{}\n--- stderr\n{}",
            self.stdout.trim_end(),
            self.stderr.trim_end()
        )
    }
}

/// Runs `cmd` with stdin closed, killing it if it runs longer than
/// `timeout`.
///
/// The command runs in a process group of its own, and the deadline kills
/// the whole group: `git` starts helpers (`git-remote-http`, `index-pack`)
/// that would otherwise outlive it and hold its pipes open.
pub fn run_for(cmd: &mut Command, timeout: Duration) -> io::Result<Finished> {
    use std::os::unix::process::CommandExt as _;
    let mut child = cmd
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let out = drain(child.stdout.take());
    let err = drain(child.stderr.take());
    let deadline = Instant::now() + timeout;
    let mut pause = Duration::from_millis(1);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break Some(status);
        }
        if Instant::now() >= deadline {
            kill_group(child.id());
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        thread::sleep(pause);
        pause = (pause * 2).min(Duration::from_millis(20));
    };
    Ok(Finished {
        status,
        stdout: out.join().unwrap_or_default(),
        stderr: err.join().unwrap_or_default(),
    })
}

/// Sends SIGKILL to process group `pgid`.
pub(crate) fn kill_group(pgid: u32) {
    let _ = Command::new("kill")
        .args(["-KILL", "--", &format!("-{pgid}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn drain<R: Read + Send + 'static>(pipe: Option<R>) -> thread::JoinHandle<String> {
    thread::spawn(move || {
        let mut text = Vec::new();
        if let Some(mut pipe) = pipe {
            let _ = pipe.read_to_end(&mut text);
        }
        String::from_utf8_lossy(&text).into_owned()
    })
}

/// Runs `cmd` with a deadline and returns its stdout, or an error that
/// holds the command line and both outputs.
pub fn run_ok(cmd: &mut Command, timeout: Duration) -> io::Result<String> {
    let done = run_for(cmd, timeout)?;
    if done.success() {
        Ok(done.stdout)
    } else {
        Err(io::Error::other(format!("{cmd:?}: {}", done.describe())))
    }
}

/// A free TCP port on 127.0.0.1. The port is free when this returns;
/// callers bind it soon after.
pub fn free_port() -> io::Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_command_past_its_deadline_is_killed() {
        let started = Instant::now();
        let done = run_for(
            Command::new("sh").args(["-c", "echo started; sleep 30"]),
            Duration::from_millis(300),
        )
        .unwrap();
        assert!(done.status.is_none());
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn output_and_status_are_kept() {
        let done = run_for(
            Command::new("sh").args(["-c", "echo out; echo err >&2; exit 3"]),
            Duration::from_secs(10),
        )
        .unwrap();
        assert_eq!(done.code(), Some(3));
        assert_eq!(done.stdout, "out\n");
        assert_eq!(done.stderr, "err\n");
    }
}
