//! Fault injectors: what happens to the deployment while clients work.
//!
//! An injector takes a step every so often during an epoch's active phase
//! and heals everything when the phase ends. Each scenario has one; `mixed`
//! picks a random step from all of them.

use std::io;
use std::time::Duration;

use aspen_testkit::rng::Rng;

use crate::cluster::Cluster;
use crate::s3proxy::S3Faults;

/// Something that breaks the deployment, a step at a time.
pub trait Injector: Send {
    /// A short name for reports.
    fn name(&self) -> &'static str;

    /// Takes one fault step. Returns a label for the step (counted in the
    /// report), or `None` if it did nothing.
    fn step(&mut self, rng: &mut Rng) -> io::Result<Option<String>>;

    /// Undoes every fault. Runs when the epoch's active phase ends.
    fn heal(&mut self) -> io::Result<()>;

    /// Whether the injector is meant to inject anything (the `calm`
    /// baseline is not). A run whose injector is meant to but injected no
    /// fault fails as vacuous.
    fn injects(&self) -> bool {
        true
    }

    /// Parts of a composite injector that have not injected a fault yet.
    /// A run fails as vacuous if any remain at its end.
    fn idle_parts(&self) -> Vec<&'static str> {
        Vec::new()
    }
}

/// The scenarios, by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    /// No faults: a baseline for the checks.
    Calm,
    /// SIGKILL instances at random while pushes are in flight.
    Kill,
    /// SIGKILL the instance holding a compaction or indexing job lease.
    Compaction,
    /// S3 errors, closes before and after a write, truncated reads,
    /// latency and partitions between Aspen and the bucket.
    S3,
    /// Partitions, connection closes and latency between Aspen and Postgres, and
    /// Postgres crash-restarts.
    Db,
    /// Postgres failover to a synchronous standby.
    Failover,
    /// Partitions, connection closes, refusals and latency between clients and Aspen.
    Net,
    /// All of the above, interleaved.
    Mixed,
    /// Each fault but the compaction kill once, in turn: the short run
    /// required CI does. (Workers poll every 5 s and a job takes
    /// milliseconds, so a 12-second run cannot count on catching one
    /// leased; `compaction` and `mixed` must, nightly.)
    Smoke,
}

impl Scenario {
    pub const ALL: [Scenario; 9] = [
        Self::Calm,
        Self::Kill,
        Self::Compaction,
        Self::S3,
        Self::Db,
        Self::Failover,
        Self::Net,
        Self::Mixed,
        Self::Smoke,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Calm => "calm",
            Self::Kill => "kill",
            Self::Compaction => "compaction",
            Self::S3 => "s3",
            Self::Db => "db",
            Self::Failover => "failover",
            Self::Net => "net",
            Self::Mixed => "mixed",
            Self::Smoke => "smoke",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|x| x.name() == s)
    }

    pub fn describe(self) -> &'static str {
        match self {
            Self::Calm => "no faults; every check must pass on a healthy deployment",
            Self::Kill => "SIGKILL random instances (sometimes all) during pushes; restart them",
            Self::Compaction => {
                "SIGKILL the instance holding a compact or index_commits lease, mid-job"
            }
            Self::S3 => {
                "S3 503/500 answers, closes before and after a write lands, truncated reads, latency, partitions"
            }
            Self::Db => {
                "database partitions, connection closes, latency, and Postgres crash-restarts"
            }
            Self::Failover => "crash the Postgres primary and promote its synchronous standby",
            Self::Net => "client-side partitions, connection closes, refusals and latency",
            Self::Mixed => "every fault above, interleaved at random",
            Self::Smoke => {
                "kill, failover, S3, database and client network faults once each, in turn"
            }
        }
    }

    /// Whether the scenario needs a database standby.
    pub fn needs_standby(self) -> bool {
        matches!(self, Self::Failover | Self::Mixed | Self::Smoke)
    }

    /// Whether the scenario needs an S3 store (the local file store cannot
    /// be faulted over the network). Its S3 faults fail as a fault-step
    /// error without one, so a run never passes with them silently off.
    pub fn needs_s3(self) -> bool {
        matches!(self, Self::S3 | Self::Mixed | Self::Smoke)
    }

    /// The scenario's injector against `cluster`.
    pub fn injector(self, cluster: &Cluster) -> Box<dyn Injector + '_> {
        match self {
            Self::Calm => Box::new(Calm),
            Self::Kill => Box::new(Kill { cluster }),
            Self::Compaction => Box::new(CompactionKill { cluster }),
            Self::S3 => Box::new(S3 { cluster, turn: 0 }),
            Self::Db => Box::new(Db { cluster, turn: 0 }),
            Self::Failover => Box::new(Failover { cluster }),
            Self::Net => Box::new(Net { cluster, turn: 0 }),
            Self::Smoke => Box::new(Mixed::new(
                true,
                vec![
                    Box::new(Kill { cluster }),
                    Box::new(Failover { cluster }),
                    Box::new(S3 { cluster, turn: 0 }),
                    Box::new(Db { cluster, turn: 0 }),
                    Box::new(Net { cluster, turn: 0 }),
                ],
            )),
            Self::Mixed => Box::new(Mixed::new(
                false,
                vec![
                    Box::new(Kill { cluster }),
                    Box::new(CompactionKill { cluster }),
                    Box::new(S3 { cluster, turn: 0 }),
                    Box::new(Db { cluster, turn: 0 }),
                    Box::new(Failover { cluster }),
                    Box::new(Net { cluster, turn: 0 }),
                ],
            )),
        }
    }
}

/// A uniformly random duration in `[lo, hi]`.
pub fn between(rng: &mut Rng, lo: Duration, hi: Duration) -> Duration {
    let span = hi.saturating_sub(lo).as_millis() as u64;
    lo + Duration::from_millis(if span == 0 {
        0
    } else {
        rng.next_u64() % (span + 1)
    })
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

struct Calm;

impl Injector for Calm {
    fn name(&self) -> &'static str {
        "calm"
    }
    fn step(&mut self, _: &mut Rng) -> io::Result<Option<String>> {
        Ok(None)
    }
    fn heal(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn injects(&self) -> bool {
        false
    }
}

struct Kill<'a> {
    cluster: &'a Cluster,
}

impl Injector for Kill<'_> {
    fn name(&self) -> &'static str {
        "kill"
    }
    fn step(&mut self, rng: &mut Rng) -> io::Result<Option<String>> {
        let n = self.cluster.instance_count();
        let label = if rng.below(6) == 0 {
            for i in 0..n {
                self.cluster.kill_instance(i)?;
            }
            "kill every instance"
        } else {
            let i = rng.below(n);
            if !self.cluster.kill_instance(i)? {
                return Ok(None);
            }
            "kill one instance"
        };
        std::thread::sleep(between(rng, ms(100), ms(2000)));
        for i in 0..n {
            self.cluster.start_instance(i)?;
        }
        Ok(Some(label.into()))
    }
    fn heal(&mut self) -> io::Result<()> {
        self.cluster.heal()
    }
}

struct CompactionKill<'a> {
    cluster: &'a Cluster,
}

impl Injector for CompactionKill<'_> {
    fn name(&self) -> &'static str {
        "compaction kill"
    }
    fn step(&mut self, rng: &mut Rng) -> io::Result<Option<String>> {
        // Watch the job table for up to 3 s for a leased job, then kill the
        // process that holds it.
        let deadline = std::time::Instant::now() + ms(3000);
        while std::time::Instant::now() < deadline {
            let running = self.cluster.running_jobs()?;
            let live: Vec<_> = running
                .into_iter()
                .filter(|(_, i, pid)| self.cluster.pid(*i) == Some(*pid))
                .collect();
            if !live.is_empty() {
                let (kind, i, _) = live[rng.below(live.len())].clone();
                self.cluster.kill_instance(i)?;
                std::thread::sleep(between(rng, ms(100), ms(1500)));
                self.cluster.start_instance(i)?;
                return Ok(Some(format!("kill during {kind}")));
            }
            std::thread::sleep(ms(10));
        }
        Ok(None)
    }
    fn heal(&mut self) -> io::Result<()> {
        self.cluster.heal()
    }
}

struct S3<'a> {
    cluster: &'a Cluster,
    /// Which kind of fault comes next: they take turns, so every kind
    /// shows up even in a short run.
    turn: usize,
}

impl Injector for S3<'_> {
    fn name(&self) -> &'static str {
        "s3"
    }
    fn step(&mut self, rng: &mut Rng) -> io::Result<Option<String>> {
        let Some(s3) = self.cluster.s3_proxy() else {
            return Err(io::Error::other(
                "S3 faults need an S3 store, and this deployment uses the local file store",
            ));
        };
        self.turn += 1;
        let label = match (self.turn + 2) % 4 {
            0 => {
                s3.stall(true);
                std::thread::sleep(between(rng, ms(500), ms(4000)));
                s3.stall(false);
                "s3 partition"
            }
            1 => {
                s3.set_faults(S3Faults {
                    delayed: 0.8,
                    max_delay: ms(800),
                    ..S3Faults::NONE
                });
                "s3 latency"
            }
            _ => {
                s3.set_faults(S3Faults {
                    slow_down: 0.06,
                    internal_error: 0.04,
                    close_before: 0.04,
                    close_after: 0.06,
                    truncate: 0.04,
                    delayed: 0.3,
                    max_delay: ms(300),
                });
                "s3 error mix"
            }
        };
        Ok(Some(label.into()))
    }
    fn heal(&mut self) -> io::Result<()> {
        if let Some(s3) = self.cluster.s3_proxy() {
            s3.heal();
        }
        Ok(())
    }
}

struct Db<'a> {
    cluster: &'a Cluster,
    /// Which kind of fault comes next: they take turns, so every kind
    /// shows up even in a short run.
    turn: usize,
}

impl Injector for Db<'_> {
    fn name(&self) -> &'static str {
        "db"
    }
    fn step(&mut self, rng: &mut Rng) -> io::Result<Option<String>> {
        let p = self.cluster.db_proxy();
        self.turn += 1;
        let label = match self.turn % 5 {
            0 => {
                p.partition(true);
                std::thread::sleep(between(rng, ms(500), ms(5000)));
                p.partition(false);
                "db partition"
            }
            1 => {
                p.partition(true);
                std::thread::sleep(between(rng, ms(500), ms(3000)));
                // A long partition: TCP gives up and the connections die.
                p.close_all();
                p.partition(false);
                "db partition then close"
            }
            2 => {
                p.close_all();
                "db connections closed"
            }
            3 => {
                p.delay(between(rng, ms(20), ms(200)));
                "db latency"
            }
            _ => {
                if self.cluster.has_standby() {
                    // A crash of a primary with a synchronous standby is a
                    // failover's job; crash-restart only a lone primary.
                    p.close_all();
                    "db connections closed"
                } else {
                    self.cluster
                        .crash_and_restart_database(between(rng, ms(200), ms(3000)))?;
                    "postgres crash-restart"
                }
            }
        };
        Ok(Some(label.into()))
    }
    fn heal(&mut self) -> io::Result<()> {
        self.cluster.db_proxy().heal();
        Ok(())
    }
}

struct Failover<'a> {
    cluster: &'a Cluster,
}

impl Injector for Failover<'_> {
    fn name(&self) -> &'static str {
        "failover"
    }
    fn step(&mut self, _: &mut Rng) -> io::Result<Option<String>> {
        Ok(self
            .cluster
            .fail_over()?
            .map(|_| "postgres failover".to_owned()))
    }
    fn heal(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Net<'a> {
    cluster: &'a Cluster,
    /// Which kind of fault comes next: they take turns, so every kind
    /// shows up even in a short run.
    turn: usize,
}

impl Injector for Net<'_> {
    fn name(&self) -> &'static str {
        "net"
    }
    fn step(&mut self, rng: &mut Rng) -> io::Result<Option<String>> {
        let p = self.cluster.front();
        self.turn += 1;
        let label = match self.turn % 4 {
            0 => {
                p.partition(true);
                std::thread::sleep(between(rng, ms(300), ms(3000)));
                p.partition(false);
                "client partition"
            }
            1 => {
                p.close_all();
                "client connections closed"
            }
            2 => {
                p.refuse(true);
                std::thread::sleep(between(rng, ms(200), ms(1500)));
                p.refuse(false);
                "client connections refused"
            }
            _ => {
                p.delay(between(rng, ms(10), ms(150)));
                "client latency"
            }
        };
        Ok(Some(label.into()))
    }
    fn heal(&mut self) -> io::Result<()> {
        self.cluster.front().heal();
        Ok(())
    }
}

struct Mixed<'a> {
    parts: Vec<Box<dyn Injector + 'a>>,
    /// Faults each part injected.
    hits: Vec<u64>,
    /// Take the parts in order instead of at random. A part that finds
    /// nothing to do (no leased job to kill yet) gets the next step too,
    /// so each part injects once before the order moves on.
    in_turn: bool,
    next: usize,
}

impl<'a> Mixed<'a> {
    fn new(in_turn: bool, parts: Vec<Box<dyn Injector + 'a>>) -> Self {
        Self {
            hits: vec![0; parts.len()],
            parts,
            in_turn,
            next: 0,
        }
    }
}

impl Injector for Mixed<'_> {
    fn name(&self) -> &'static str {
        "mixed"
    }
    fn step(&mut self, rng: &mut Rng) -> io::Result<Option<String>> {
        let idle: Vec<usize> = (0..self.parts.len())
            .filter(|&i| self.hits[i] == 0)
            .collect();
        let i = if self.in_turn {
            self.next % self.parts.len()
        } else if !idle.is_empty() && rng.below(2) == 0 {
            // Half the steps go to a part that has not injected yet, so a
            // short run still covers every part.
            idle[rng.below(idle.len())]
        } else {
            rng.below(self.parts.len())
        };
        let r = self.parts[i].step(rng);
        if matches!(r, Ok(Some(_))) {
            self.hits[i] += 1;
            if self.in_turn {
                self.next += 1;
            }
        }
        r
    }
    fn heal(&mut self) -> io::Result<()> {
        for p in &mut self.parts {
            p.heal()?;
        }
        Ok(())
    }
    fn idle_parts(&self) -> Vec<&'static str> {
        self.parts
            .iter()
            .zip(&self.hits)
            .filter(|(_, h)| **h == 0)
            .map(|(p, _)| p.name())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A part that injects every step, or never.
    struct Part {
        name: &'static str,
        hits: bool,
    }

    impl Injector for Part {
        fn name(&self) -> &'static str {
            self.name
        }
        fn step(&mut self, _: &mut Rng) -> io::Result<Option<String>> {
            Ok(self.hits.then(|| self.name.to_owned()))
        }
        fn heal(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn parts() -> Vec<Box<dyn Injector>> {
        vec![
            Box::new(Part {
                name: "works",
                hits: true,
            }),
            Box::new(Part {
                name: "never",
                hits: false,
            }),
            Box::new(Part {
                name: "also works",
                hits: true,
            }),
        ]
    }

    #[test]
    fn a_mixed_part_that_never_injects_stays_idle() {
        let mut rng = Rng::new(7);
        let mut mixed = Mixed::new(false, parts());
        assert_eq!(mixed.idle_parts(), ["works", "never", "also works"]);
        let injected = (0..200)
            .filter(|_| matches!(mixed.step(&mut rng), Ok(Some(_))))
            .count();
        assert!(injected > 0);
        assert_eq!(mixed.idle_parts(), ["never"]);
    }

    #[test]
    fn smoke_takes_parts_in_turn_and_waits_on_one_that_finds_nothing() {
        let mut rng = Rng::new(7);
        let mut smoke = Mixed::new(true, parts());
        // The first part injects; then the order stops at the part that
        // finds nothing, so the third part is never reached.
        assert_eq!(smoke.step(&mut rng).unwrap().as_deref(), Some("works"));
        for _ in 0..10 {
            assert_eq!(smoke.step(&mut rng).unwrap(), None);
        }
        assert_eq!(smoke.idle_parts(), ["never", "also works"]);
    }
}
