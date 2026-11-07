//! Personas: a roster of pretend painters who drop in on the canvas like
//! people do, so it always looks alive.
//!
//! Each persona has a name, a pseudonymous commit author
//! (`Name <slug@personas.place.invalid>`) and one or two pictures it loves
//! to draw. While it is on a shift it paints one picture tile by tile, one
//! tile per push, as a visitor clicking on the page would, at a human pace
//! (one push every 1 to 5 seconds, with the odd longer pause). When the
//! picture is done it either draws something small nearby or, once its
//! shift is over, leaves. Shifts last a few minutes to about twenty, and
//! who is on changes over time: at least [`PersonaSettings::min_active`]
//! personas are always on and at most [`PersonaSettings::max_active`].
//!
//! Personas push on their own branches, `agent/<slug>-<n>` (a new `n` per
//! shift), so the server's rules and the live feed treat them as agents,
//! never as people.
//!
//! They respect people: a persona never paints a tile a person painted in
//! the last six hours or a keep area. It places each picture where it
//! covers none of those, and if a person paints inside its picture's area
//! before it is done, it starts the picture again somewhere else.
//!
//! [`Director`] is the whole behaviour as a pure state machine over an
//! explicit clock: [`Director::tick`] says which tiles to push now, and
//! [`Director::finished`] reports how each push ended. `place` drives it
//! from its agents' tick; the tests drive it with a simulated clock.

use std::collections::HashSet;

use crate::rules::{Rules, Tile};
use crate::sprites::{self, Sprite};

/// How the personas behave. The defaults are what the public demo runs.
#[derive(Debug, Clone, PartialEq)]
pub struct PersonaSettings {
    /// Personas always on shift (at least 1).
    pub min_active: usize,
    /// Personas on shift at most (at least `min_active`, at most the
    /// roster's size).
    pub max_active: usize,
    /// Pushes per second all personas together may make, on average; a
    /// burst of up to one second's worth (at least one push) may go at
    /// once.
    pub pushes_per_second: f64,
    /// The time between one persona's pushes is uniform in this range, ms.
    pub push_gap_ms: (u64, u64),
    /// After a push, the chance in percent that a persona pauses longer,
    /// for a time uniform in [`Self::pause_ms`].
    pub pause_percent: u64,
    /// A longer pause, ms.
    pub pause_ms: (u64, u64),
    /// A shift lasts a time uniform in this range, ms. A persona whose
    /// shift is over finishes the picture it is on before it leaves.
    pub shift_ms: (u64, u64),
    /// Mean time between personas dropping in while fewer than
    /// `max_active` are on, ms (each wait is uniform from half to one and
    /// a half times this).
    pub arrival_mean_ms: u64,
    /// A persona that left does not come back for at least this long,
    /// unless nobody else is free, ms.
    pub rest_ms: u64,
    /// While the server-wide push ceiling is reached, every persona waits
    /// this long (plus up to the same again, at random) before it tries
    /// again, ms.
    pub backoff_ms: u64,
}

impl Default for PersonaSettings {
    fn default() -> Self {
        Self {
            min_active: 3,
            max_active: 6,
            pushes_per_second: 3.0,
            push_gap_ms: (1000, 5000),
            pause_percent: 5,
            pause_ms: (6000, 15_000),
            shift_ms: (3 * 60_000, 20 * 60_000),
            arrival_mean_ms: 2 * 60_000,
            rest_ms: 10 * 60_000,
            backoff_ms: 5000,
        }
    }
}

impl PersonaSettings {
    /// Checks the settings against the roster's size.
    pub fn validate(&self, roster: usize) -> Result<(), String> {
        if self.min_active == 0 || self.min_active > self.max_active {
            return Err(format!(
                "PLACE_PERSONAS_MIN: must be at least 1 and at most PLACE_PERSONAS_MAX ({}), got {}",
                self.max_active, self.min_active
            ));
        }
        if self.max_active > roster {
            return Err(format!(
                "PLACE_PERSONAS_MAX: at most {roster} personas can be on at once (the roster's size), got {}",
                self.max_active
            ));
        }
        if !(self.pushes_per_second.is_finite() && self.pushes_per_second > 0.0) {
            return Err(format!(
                "PLACE_PERSONAS_PUSHES_PER_SECOND: must be a number above 0, got {}",
                self.pushes_per_second
            ));
        }
        let ordered = |(a, b): (u64, u64)| a <= b;
        if !ordered(self.push_gap_ms)
            || !ordered(self.pause_ms)
            || !ordered(self.shift_ms)
            || self.pause_percent > 100
        {
            return Err("personas: every range must be low to high".into());
        }
        Ok(())
    }
}

/// A pretend painter.
#[derive(Debug)]
pub struct Persona {
    /// Branch and e-mail name: lowercase letters only.
    pub slug: &'static str,
    /// The name shown in the feed and in its commits.
    pub name: &'static str,
    /// What it loves to draw; the first is its favourite.
    pub loves: &'static [&'static Sprite],
}

impl Persona {
    /// The commit author of its pushes. The `.invalid` domain makes it
    /// plain that nobody reads that mailbox.
    pub fn author(&self) -> String {
        format!("{} <{}@personas.place.invalid>", self.name, self.slug)
    }
}

// Colors of the default palette (`rules::default_palette`).
const WHITE: &str = "#ffffff";
const BLACK: &str = "#1b1f22";
const PINK: &str = "#ff99aa";
const RED: &str = "#d6262b";
const BROWN: &str = "#9c6926";
const YELLOW: &str = "#ffd635";
const ORANGE: &str = "#ff7a1a";
const LIME: &str = "#7eed56";
const GREEN: &str = "#00a368";
const BLUE: &str = "#3690ea";
const NAVY: &str = "#2f3ab4";
const MAGENTA: &str = "#b44ac0";

/// The pictures only personas draw (they draw the ambient sprites too).
#[rustfmt::skip]
pub mod pictures {
    use super::*;

    /// California's flag, simplified: a star, a walking bear on a strip of
    /// grass, and the red stripe along the bottom.
    pub const CALIFORNIA: Sprite = Sprite {
        name: "california-flag",
        art: &[
            "1112111111111111",
            "1222221111111111",
            "1122211111111111",
            "1121211333333111",
            "1113113333333311",
            "1133333333333331",
            "1113333333333311",
            "1111331133113311",
            "1144444444444441",
            "2222222222222222",
            "2222222222222222",
        ],
        variants: &[&[WHITE, RED, BROWN, GREEN]],
    };

    /// Sweden's flag: a yellow Nordic cross on blue.
    pub const SWEDEN: Sprite = Sprite {
        name: "swedish-flag",
        art: &[
            "1111122111111111",
            "1111122111111111",
            "1111122111111111",
            "1111122111111111",
            "2222222222222222",
            "2222222222222222",
            "1111122111111111",
            "1111122111111111",
            "1111122111111111",
            "1111122111111111",
        ],
        variants: &[&[BLUE, YELLOW]],
    };

    /// Norway's flag: a blue Nordic cross edged in white on red.
    pub const NORWAY: Sprite = Sprite {
        name: "norwegian-flag",
        art: &[
            "1111233211111111",
            "1111233211111111",
            "1111233211111111",
            "1111233211111111",
            "2222233222222222",
            "3333333333333333",
            "3333333333333333",
            "2222233222222222",
            "1111233211111111",
            "1111233211111111",
            "1111233211111111",
            "1111233211111111",
        ],
        variants: &[&[RED, WHITE, NAVY]],
    };

    /// A green frog sitting, seen from the front.
    pub const FROG: Sprite = Sprite {
        name: "frog",
        art: &[
            ".333.....333.",
            "34331...13343",
            "3333111113333",
            ".11111111111.",
            "1111111111111",
            "1122222222211",
            "1111222221111",
            ".11.11111.11.",
            "111.......111",
        ],
        variants: &[&[GREEN, LIME, WHITE, BLACK]],
    };

    /// A sunflower on its stem.
    pub const SUNFLOWER: Sprite = Sprite {
        name: "sunflower",
        art: &[
            "..1.1.1..",
            ".1111111.",
            "111222111",
            ".1222221.",
            "112222211",
            ".1222221.",
            "111222111",
            ".1111111.",
            "..1.3.1..",
            "....3.44.",
            ".44.3444.",
            ".4443.4..",
            "....3....",
        ],
        variants: &[&[YELLOW, BROWN, GREEN, LIME], &[ORANGE, BROWN, GREEN, LIME]],
    };

    /// "HEJ", Swedish for hi.
    pub const HEJ: Sprite = Sprite {
        name: "hej",
        art: &[
            "1.1.111...1",
            "1.1.1.....1",
            "111.11....1",
            "1.1.1...1.1",
            "1.1.111..1.",
        ],
        variants: &[&[NAVY], &[BLUE], &[MAGENTA]],
    };

    /// "HEI", Norwegian for hi.
    pub const HEI: Sprite = Sprite {
        name: "hei",
        art: &[
            "1.1.111.111",
            "1.1.1....1.",
            "111.11...1.",
            "1.1.1....1.",
            "1.1.111.111",
        ],
        variants: &[&[RED], &[NAVY]],
    };

    /// "GIT".
    pub const GIT: Sprite = Sprite {
        name: "git",
        art: &[
            ".11.111.111",
            "1....1...1.",
            "1.1..1...1.",
            "1.1..1...1.",
            ".11.111..1.",
        ],
        variants: &[&[ORANGE], &[BLACK], &[GREEN]],
    };

    /// "HI!".
    pub const HI: Sprite = Sprite {
        name: "hi",
        art: &[
            "1.1.111.2",
            "1.1..1..2",
            "111..1..2",
            "1.1..1...",
            "1.1.111.2",
        ],
        variants: &[&[BLUE, ORANGE], &[MAGENTA, YELLOW]],
    };

    /// A small heart, for a quick doodle next to a finished picture.
    pub const SMALL_HEART: Sprite = Sprite {
        name: "small-heart",
        art: &[
            "11.11",
            "11111",
            ".111.",
            "..1..",
        ],
        variants: &[&[RED], &[PINK], &[MAGENTA]],
    };

    /// A small flower, for a quick doodle next to a finished picture.
    pub const SMALL_FLOWER: Sprite = Sprite {
        name: "small-flower",
        art: &[
            ".1.",
            "121",
            ".1.",
            ".3.",
        ],
        variants: &[&[PINK, YELLOW, GREEN], &[WHITE, YELLOW, GREEN], &[MAGENTA, YELLOW, GREEN]],
    };

    /// Every picture here, for tests and renders.
    pub const ALL: &[&Sprite] = &[
        &CALIFORNIA, &SWEDEN, &NORWAY, &FROG, &SUNFLOWER, &HEJ, &HEI, &GIT, &HI,
        &SMALL_HEART, &SMALL_FLOWER,
    ];

    /// Doodles a persona draws next to a picture it finished.
    pub const SMALL: &[&Sprite] = &[&SMALL_HEART, &SMALL_FLOWER];
}

const fn sprite(name: &str) -> &'static Sprite {
    let mut i = 0;
    while i < sprites::SPRITES.len() {
        let s = &sprites::SPRITES[i];
        if s.name.len() == name.len() {
            let (a, b) = (s.name.as_bytes(), name.as_bytes());
            let mut j = 0;
            while j < a.len() && a[j] == b[j] {
                j += 1;
            }
            if j == a.len() {
                return s;
            }
        }
        i += 1;
    }
    panic!("no such sprite")
}

const HEART: &Sprite = sprite("heart");
const HOUSE: &Sprite = sprite("house");
const CAT: &Sprite = sprite("cat");
const ROCKET: &Sprite = sprite("rocket");
const SMILEY: &Sprite = sprite("smiley");
const STAR: &Sprite = sprite("star");
const TREE: &Sprite = sprite("tree");
const BRANCH: &Sprite = sprite("git-branch");

/// The built-in roster.
pub const ROSTER: &[Persona] = &[
    Persona {
        slug: "maja",
        name: "Maja",
        loves: &[&pictures::SWEDEN, &pictures::HEJ],
    },
    Persona {
        slug: "rosa",
        name: "Rosa",
        loves: &[&pictures::CALIFORNIA, &pictures::SUNFLOWER],
    },
    Persona {
        slug: "pip",
        name: "Pip",
        loves: &[&pictures::FROG, &pictures::SMALL_HEART],
    },
    Persona {
        slug: "theo",
        name: "Theo",
        loves: &[HOUSE, TREE],
    },
    Persona {
        slug: "ingrid",
        name: "Ingrid",
        loves: &[&pictures::NORWAY, &pictures::HEI],
    },
    Persona {
        slug: "juno",
        name: "Juno",
        loves: &[ROCKET, STAR],
    },
    Persona {
        slug: "kit",
        name: "Kit",
        loves: &[CAT, SMILEY],
    },
    Persona {
        slug: "sol",
        name: "Sol",
        loves: &[&pictures::SUNFLOWER, SMILEY],
    },
    Persona {
        slug: "ash",
        name: "Ash",
        loves: &[&pictures::GIT, BRANCH],
    },
    Persona {
        slug: "bea",
        name: "Bea",
        loves: &[HEART, &pictures::SMALL_FLOWER],
    },
    Persona {
        slug: "otto",
        name: "Otto",
        loves: &[HOUSE, &pictures::FROG],
    },
    Persona {
        slug: "lumi",
        name: "Lumi",
        loves: &[STAR, ROCKET],
    },
    Persona {
        slug: "remy",
        name: "Remy",
        loves: &[SMILEY, &pictures::HI],
    },
];

/// What the canvas looks like to the personas at one tick.
#[derive(Debug)]
pub struct View<'a> {
    /// Canvas size and palette.
    pub rules: &'a Rules,
    /// Tiles a persona must never paint: what people painted in the last
    /// six hours, and the keep areas.
    pub people: &'a HashSet<usize>,
    /// Tiles the server's agents painted recently (personas included);
    /// pictures go elsewhere when they can, but may cover these.
    pub agent_paint: &'a HashSet<usize>,
    /// `main`'s tiles (palette index, or [`crate::state::UNSET`]); a tile
    /// that already has the right color is skipped.
    pub pixels: &'a [u8],
    /// The server-wide push ceiling is reached: nobody pushes, everyone
    /// waits ([`PersonaSettings::backoff_ms`]).
    pub ceiling_busy: bool,
}

/// One push to make now: exactly one tile, as a click on the page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stroke {
    /// Which persona ([`ROSTER`] index); hand it back to
    /// [`Director::finished`].
    pub persona: usize,
    /// Its branch, `agent/<slug>-<n>`.
    pub branch: String,
    /// Its name.
    pub name: &'static str,
    /// The commit author.
    pub author: String,
    /// The picture it is painting.
    pub picture: &'static str,
    /// The tile.
    pub tile: Tile,
}

/// A rectangle of tiles: `(x, y, width, height)`.
pub type Rect = (u32, u32, u32, u32);

fn overlap(a: Rect, b: Rect) -> u32 {
    let w = (a.0 + a.2).min(b.0 + b.2).saturating_sub(a.0.max(b.0));
    let h = (a.1 + a.3).min(b.1 + b.3).saturating_sub(a.1.max(b.1));
    w * h
}

/// A picture a persona is painting.
#[derive(Debug, Clone)]
struct Job {
    sprite: &'static Sprite,
    variant: usize,
    rect: Rect,
    /// In painting order.
    tiles: Vec<Tile>,
    /// Tiles before this one are painted.
    next: usize,
}

#[derive(Debug, Clone)]
struct Shift {
    branch: String,
    ends_ms: u64,
    job: Option<Job>,
    /// The last finished picture, to draw something small near it.
    last_rect: Option<Rect>,
    next_push_ms: u64,
    /// The tile of the push in flight, if any.
    in_flight: Option<Tile>,
    failures: u32,
}

#[derive(Debug, Clone)]
struct Seat {
    shift: Option<Shift>,
    /// Shifts started, for the branch name.
    shifts: u64,
    /// Not before this unless nobody else is free.
    rested_ms: u64,
}

/// A picture a persona finished: every tile of it was pushed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completed {
    /// [`ROSTER`] index.
    pub persona: usize,
    /// The picture's name.
    pub picture: &'static str,
    /// Its tiles.
    pub tiles: Vec<Tile>,
}

/// The most corners one placement scans after its 64 random tries: enough
/// for any picture anywhere on a 64x64 canvas (at most 3,782 corners), and
/// on a bigger one a window starting at a random corner. Each corner does
/// up to 192 lookups in the people and keep sets, and a corner that passes
/// that check up to 192 more in `agent_paint`, so 384 at most. The bound is
/// per placement, not per tick: one step may try about 5 sprites, and up to
/// 6 personas step per tick while the director's lock is held.
pub const SCAN_CORNERS: u64 = 4096;

/// The personas' behaviour (module docs).
#[derive(Debug)]
pub struct Director {
    roster: &'static [Persona],
    settings: PersonaSettings,
    seats: Vec<Seat>,
    rng: u64,
    next_arrival_ms: u64,
    /// Push credit, in pushes.
    credit: f64,
    credit_ms: u64,
    /// Pictures finished since [`Director::take_completed`] last ran (the
    /// newest 64).
    completed: Vec<Completed>,
    /// Pictures moved because a person painted inside them.
    pub relocations: u64,
    /// Finished pictures no persona paints over until the time given, ms
    /// ([`FRESH_MS`]).
    fresh: Vec<(Rect, u64)>,
}

/// How long a finished picture is left alone by the other personas.
pub const FRESH_MS: u64 = 10 * 60_000;

impl Director {
    /// A director for `roster` starting at `now` (ms); `seed` makes it
    /// repeatable.
    pub fn new(
        roster: &'static [Persona],
        settings: PersonaSettings,
        seed: u64,
        now: u64,
    ) -> Result<Self, String> {
        settings.validate(roster.len())?;
        let mut d = Self {
            roster,
            seats: roster
                .iter()
                .enumerate()
                .map(|(i, _)| Seat {
                    shift: None,
                    // Wide, so two tasks side by side (a start-first deploy)
                    // practically never name the same branch.
                    shifts: (seed.rotate_left(i as u32 * 7) % 1_000_000_000) + 10,
                    rested_ms: 0,
                })
                .collect(),
            rng: seed,
            next_arrival_ms: now,
            credit: 1.0,
            credit_ms: now,
            completed: Vec::new(),
            relocations: 0,
            fresh: Vec::new(),
            settings,
        };
        d.next_arrival_ms = now + d.arrival_wait();
        Ok(d)
    }

    fn next(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }

    fn between(&mut self, (lo, hi): (u64, u64)) -> u64 {
        lo + self.below(hi - lo + 1)
    }

    fn arrival_wait(&mut self) -> u64 {
        let m = self.settings.arrival_mean_ms.max(2);
        m / 2 + self.below(m)
    }

    /// The settings.
    pub fn settings(&self) -> &PersonaSettings {
        &self.settings
    }

    /// Personas on shift now, by [`ROSTER`] index, with their branch.
    pub fn active(&self) -> Vec<(usize, &str)> {
        self.seats
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.shift.as_ref().map(|sh| (i, sh.branch.as_str())))
            .collect()
    }

    /// The area of each picture being painted now: `(persona, picture,
    /// rect)`.
    pub fn pictures(&self) -> Vec<(usize, &'static str, Rect)> {
        self.seats
            .iter()
            .enumerate()
            .filter_map(|(i, s)| {
                let job = s.shift.as_ref()?.job.as_ref()?;
                Some((i, job.sprite.name, job.rect))
            })
            .collect()
    }

    /// Pictures finished since the last call (at most the newest 64).
    pub fn take_completed(&mut self) -> Vec<Completed> {
        std::mem::take(&mut self.completed)
    }

    /// Whether [`Self::tick`] has anything to do at `now`: cheap, so the
    /// caller builds a [`View`] only when it does.
    pub fn due(&self, now: u64) -> bool {
        let active = self.active().len();
        active < self.settings.min_active
            || (active < self.settings.max_active && now >= self.next_arrival_ms)
            || self.seats.iter().any(|s| {
                s.shift
                    .as_ref()
                    .is_some_and(|sh| sh.in_flight.is_none() && sh.next_push_ms <= now)
            })
    }

    /// Starts a shift for a free persona: one rested if any, else any.
    fn arrive(&mut self, now: u64) {
        let free: Vec<usize> = (0..self.seats.len())
            .filter(|&i| self.seats[i].shift.is_none())
            .collect();
        let rested: Vec<usize> = free
            .iter()
            .copied()
            .filter(|&i| self.seats[i].rested_ms <= now)
            .collect();
        let pool = if rested.is_empty() { free } else { rested };
        if pool.is_empty() {
            return;
        }
        let i = pool[self.below(pool.len() as u64) as usize];
        let length = self.between(self.settings.shift_ms);
        let first = self.below(2000);
        let slug = self.roster[i].slug;
        let seat = &mut self.seats[i];
        seat.shifts += 1;
        seat.shift = Some(Shift {
            branch: format!("{}{slug}-{}", crate::rules::AMBIENT_PREFIX, seat.shifts),
            ends_ms: now + length,
            job: None,
            last_rect: None,
            next_push_ms: now + first,
            in_flight: None,
            failures: 0,
        });
    }

    fn leave(&mut self, i: usize, now: u64) {
        self.seats[i].shift = None;
        self.seats[i].rested_ms = now + self.settings.rest_ms;
    }

    /// Where to put `sprite`: none of its tiles over people's, never over a
    /// picture another persona is painting, preferring spots clear of
    /// recent agent paint and, with `near`, close to that rectangle.
    fn choose_spot(
        &mut self,
        view: &View<'_>,
        sprite: &'static Sprite,
        variant: usize,
        me: usize,
        near: Option<Rect>,
    ) -> Option<Job> {
        let (w, h) = sprite.size();
        let rules = view.rules;
        if w > rules.width || h > rules.height {
            return None;
        }
        let others: Vec<Rect> = self
            .pictures()
            .into_iter()
            .filter(|(p, _, _)| *p != me)
            .map(|(_, _, r)| r)
            .chain(self.fresh.iter().map(|(r, _)| *r))
            .collect();
        // The picture's tiles relative to its corner, built once: each
        // corner below is checked against people's tiles and scored
        // without allocating, and only the best one gets its tiles.
        let shape = sprite.tiles(variant, 0, 0, &rules.palette)?;
        let mut best: Option<(u64, (u32, u32))> = None;
        // 64 random corners (half of them near `near`), then, if none fits
        // (a big picture on a busy canvas), up to `SCAN_CORNERS` corners
        // in order from a random one, so a gap is found on the demo's
        // canvas while one choice stays a bounded amount of work (the
        // director's lock is held meanwhile).
        let (xs, ys) = (rules.width - w + 1, rules.height - h + 1);
        let corners = u64::from(xs) * u64::from(ys);
        let scan_from = self.below(corners);
        let scan = corners.min(SCAN_CORNERS);
        for attempt in 0..64 + scan {
            if attempt == 64 && best.is_some() {
                break;
            }
            let (x, y) = if attempt >= 64 {
                let k = (scan_from + attempt - 64) % corners;
                ((k % u64::from(xs)) as u32, (k / u64::from(xs)) as u32)
            } else {
                match near {
                    // Half the tries right around the last picture.
                    Some((nx, ny, nw, nh)) if attempt % 2 == 0 => {
                        // Corners within `around + 2` of the last one, from 0 to
                        // `last`, the last corner where the picture still fits.
                        let span = |n: u32, around: u32, last: u32| {
                            let reach = around.saturating_add(2);
                            let hi = n.saturating_add(reach).min(last);
                            let lo = n.saturating_sub(reach).min(hi);
                            (lo, hi)
                        };
                        let (xl, xh) = span(nx, w.max(nw), rules.width - w);
                        let (yl, yh) = span(ny, h.max(nh), rules.height - h);
                        (
                            (u64::from(xl) + self.below(u64::from(xh - xl + 1))) as u32,
                            (u64::from(yl) + self.below(u64::from(yh - yl + 1))) as u32,
                        )
                    }
                    _ => (
                        self.below(u64::from(xs)) as u32,
                        self.below(u64::from(ys)) as u32,
                    ),
                }
            };
            let rect = (x, y, w, h);
            if others.iter().any(|o| overlap(rect, *o) > 0) {
                continue;
            }
            let at = |t: &Tile| rules.index(x + t.x, y + t.y);
            if shape.iter().any(|t| view.people.contains(&at(t))) {
                continue;
            }
            let mut score: u64 = shape
                .iter()
                .filter(|t| view.agent_paint.contains(&at(t)))
                .count() as u64
                * 4;
            if let Some((nx, ny, _, _)) = near {
                score += u64::from(x.abs_diff(nx) + y.abs_diff(ny));
            }
            if best.as_ref().is_none_or(|(s, _)| score < *s) {
                best = Some((score, (x, y)));
            }
            if score == 0 || attempt >= 64 {
                break;
            }
        }
        let (_, (x, y)) = best?;
        let tiles = shape
            .iter()
            .map(|t| Tile {
                x: x + t.x,
                y: y + t.y,
                color: t.color,
            })
            .collect();
        Some(Job::new(sprite, variant, (x, y, w, h), tiles))
    }

    /// What to push at `now`. Each [`Stroke`] must be reported back with
    /// [`Self::finished`]; a persona makes one push at a time.
    pub fn tick(&mut self, now: u64, view: &View<'_>) -> Vec<Stroke> {
        self.fresh.retain(|(_, until)| *until > now);
        // Leave: shift over and nothing left to paint.
        for i in 0..self.seats.len() {
            let done = self.seats[i]
                .shift
                .as_ref()
                .is_some_and(|sh| now >= sh.ends_ms && sh.job.is_none() && sh.in_flight.is_none());
            if done {
                self.leave(i, now);
            }
        }
        // Arrive: never fewer than the minimum, sometimes one more.
        while self.active().len() < self.settings.min_active {
            self.arrive(now);
        }
        if now >= self.next_arrival_ms {
            if self.active().len() < self.settings.max_active {
                self.arrive(now);
            }
            self.next_arrival_ms = now + self.arrival_wait();
        }
        // Credit for the budget, at most one second's worth banked.
        let rate = self.settings.pushes_per_second;
        let cap = rate.max(1.0);
        self.credit =
            (self.credit + now.saturating_sub(self.credit_ms) as f64 * rate / 1000.0).min(cap);
        self.credit_ms = now;

        let mut out = Vec::new();
        let n = self.seats.len();
        let start = self.below(n as u64) as usize;
        for k in 0..n {
            let i = (start + k) % n;
            let Some(sh) = self.seats[i].shift.as_ref() else {
                continue;
            };
            if sh.in_flight.is_some() || sh.next_push_ms > now {
                continue;
            }
            if view.ceiling_busy {
                let wait = self.settings.backoff_ms + self.below(self.settings.backoff_ms + 1);
                self.shift_mut(i).next_push_ms = now + wait;
                continue;
            }
            if let Some(stroke) = self.step(i, now, view) {
                if self.credit < 1.0 {
                    // Over budget: try again shortly.
                    let wait = 200 + self.below(800);
                    self.shift_mut(i).next_push_ms = now + wait;
                    continue;
                }
                self.credit -= 1.0;
                let gap = self.between(self.settings.push_gap_ms);
                let pause = if self.below(100) < self.settings.pause_percent {
                    self.between(self.settings.pause_ms)
                } else {
                    0
                };
                let sh = self.shift_mut(i);
                sh.in_flight = Some(stroke.tile);
                sh.next_push_ms = now + gap + pause;
                out.push(stroke);
            }
        }
        out
    }

    fn shift_mut(&mut self, i: usize) -> &mut Shift {
        self.seats[i].shift.as_mut().expect("on shift")
    }

    /// The next tile persona `i` paints, choosing, moving or finishing its
    /// picture as needed. `None` when it has nothing to paint now.
    fn step(&mut self, i: usize, now: u64, view: &View<'_>) -> Option<Stroke> {
        let rules = view.rules;
        loop {
            let (has_job, over, near) = {
                let sh = self.seats[i].shift.as_ref().expect("on shift");
                (sh.job.is_some(), now >= sh.ends_ms, sh.last_rect)
            };
            if !has_job {
                if over {
                    // Leaves on the next tick.
                    return None;
                }
                let roster = self.roster;
                let persona = &roster[i];
                let (sprite, near) = match near {
                    // Something small nearby after a finished picture.
                    Some(r) if self.below(2) == 0 => {
                        let small = pictures::SMALL;
                        (small[self.below(small.len() as u64) as usize], Some(r))
                    }
                    _ => {
                        let loves = persona.loves;
                        // The favourite twice as often.
                        let k = self.below(loves.len() as u64 + 1) as usize;
                        (loves[k.saturating_sub(1)], None)
                    }
                };
                // When it does not fit around people's work, its other
                // loves, then a small doodle, before giving up for now.
                let small = pictures::SMALL;
                let fallbacks = persona
                    .loves
                    .iter()
                    .chain(small)
                    .filter(|s| s.name != sprite.name);
                let mut picked = None;
                for candidate in std::iter::once(&sprite).chain(fallbacks) {
                    let variant = self.below(candidate.variants.len() as u64) as usize;
                    picked = self.choose_spot(view, candidate, variant, i, near);
                    if picked.is_some() {
                        break;
                    }
                }
                match picked {
                    Some(job) => self.shift_mut(i).job = Some(job),
                    None => {
                        // No room clear of people now: look again later.
                        let wait = 10_000 + self.below(10_000);
                        self.shift_mut(i).next_push_ms = now + wait;
                        return None;
                    }
                }
            }
            let job = self.shift_mut(i).job.clone().expect("set above");
            // A person painted a tile of the picture: start again
            // somewhere else.
            let taken = job
                .tiles
                .iter()
                .any(|t| view.people.contains(&rules.index(t.x, t.y)));
            if taken {
                self.relocations += 1;
                let moved = self.choose_spot(view, job.sprite, job.variant, i, Some(job.rect));
                let sh = self.shift_mut(i);
                sh.job = moved;
                if sh.job.is_none() {
                    let wait = 20_000;
                    sh.next_push_ms = now + wait;
                    return None;
                }
                continue;
            }
            // The next tile that is not already that color.
            let mut next = job.next;
            while next < job.tiles.len() {
                let t = job.tiles[next];
                if view.pixels.get(rules.index(t.x, t.y)) != Some(&t.color) {
                    break;
                }
                next += 1;
            }
            if next >= job.tiles.len() {
                let sh = self.shift_mut(i);
                let done = sh.job.take().expect("set");
                sh.last_rect = Some(done.rect);
                self.fresh.push((done.rect, now + FRESH_MS));
                self.completed.push(Completed {
                    persona: i,
                    picture: done.sprite.name,
                    tiles: done.tiles,
                });
                if self.completed.len() > 64 {
                    self.completed.remove(0);
                }
                // A moment to admire it, then on (or off).
                let wait = 3000 + self.below(7000);
                self.shift_mut(i).next_push_ms = now + wait;
                return None;
            }
            let sh = self.shift_mut(i);
            sh.job.as_mut().expect("set").next = next;
            let branch = sh.branch.clone();
            let persona = &self.roster[i];
            return Some(Stroke {
                persona: i,
                branch,
                name: persona.name,
                author: persona.author(),
                picture: job.sprite.name,
                tile: job.tiles[next],
            });
        }
    }

    /// The push of `persona`'s last [`Stroke`] ended: committed (`ok`) or
    /// not (refused, or dropped for want of a free agent permit), in which
    /// case the same tile is retried with exponential backoff from `now`
    /// (ms), from one second up to one minute. There is no attempt limit.
    pub fn finished(&mut self, persona: usize, ok: bool, now: u64) {
        let Some(sh) = self.seats.get_mut(persona).and_then(|s| s.shift.as_mut()) else {
            return;
        };
        let Some(tile) = sh.in_flight.take() else {
            return;
        };
        if !ok {
            let wait = (1000u64 << sh.failures.min(6)).min(60_000);
            sh.failures = sh.failures.saturating_add(1);
            sh.next_push_ms = sh.next_push_ms.max(now.saturating_add(wait));
            return;
        }
        sh.failures = 0;
        if let Some(job) = sh.job.as_mut()
            && job.tiles.get(job.next) == Some(&tile)
        {
            job.next += 1;
        }
    }
}

impl Job {
    fn new(sprite: &'static Sprite, variant: usize, rect: Rect, mut tiles: Vec<Tile>) -> Self {
        // Like a person: one color at a time, row by row.
        tiles.sort_by_key(|t| (t.color, t.y, t.x));
        Self {
            sprite,
            variant,
            rect,
            tiles,
            next: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::default_palette;
    use crate::state::UNSET;

    fn rules(w: u32, h: u32) -> Rules {
        Rules {
            width: w,
            height: h,
            palette: default_palette(),
            tiles_per_push: 64,
        }
    }

    #[test]
    fn every_picture_is_rectangular_and_in_the_palette_and_every_persona_differs() {
        let palette = default_palette();
        for s in pictures::ALL
            .iter()
            .chain(ROSTER.iter().flat_map(|p| p.loves.iter()))
        {
            let (w, h) = s.size();
            assert!(s.art.iter().all(|r| r.len() as u32 == w), "{}", s.name);
            assert!((3..=16).contains(&w) && (3..=16).contains(&h), "{}", s.name);
            for v in 0..s.variants.len() {
                let tiles = s.tiles(v, 0, 0, &palette).expect(s.name);
                assert!(!tiles.is_empty(), "{}", s.name);
                let roles = s
                    .art
                    .iter()
                    .flat_map(|r| r.bytes())
                    .filter(|c| *c != b'.')
                    .max();
                assert_eq!(
                    usize::from(roles.unwrap() - b'0'),
                    s.variants[v].len(),
                    "{}",
                    s.name
                );
            }
        }
        let mut slugs: Vec<_> = ROSTER.iter().map(|p| p.slug).collect();
        slugs.sort_unstable();
        slugs.dedup();
        assert_eq!(slugs.len(), ROSTER.len());
        assert!((10..=15).contains(&ROSTER.len()));
        for p in ROSTER {
            assert!(p.slug.bytes().all(|b| b.is_ascii_lowercase()), "{}", p.slug);
            assert!(!p.loves.is_empty() && p.loves.len() <= 2, "{}", p.slug);
        }
        // the maintainers' must-haves.
        for must in ["california-flag", "swedish-flag", "house", "frog"] {
            assert!(
                ROSTER
                    .iter()
                    .any(|p| p.loves.iter().any(|s| s.name == must)),
                "{must}"
            );
        }
    }

    /// A simulated canvas: `main`'s pixels with every committed push
    /// applied at once, and people painting random tiles now and then.
    struct World {
        rules: Rules,
        pixels: Vec<u8>,
        /// Tile index to when a person painted it (ms).
        people: std::collections::HashMap<usize, u64>,
        keep: Vec<Rect>,
        agent: HashSet<usize>,
        now: u64,
        rng: u64,
        strokes: u64,
    }

    impl World {
        fn new(w: u32, h: u32) -> Self {
            Self {
                rules: rules(w, h),
                pixels: vec![UNSET; (w * h) as usize],
                people: Default::default(),
                keep: Vec::new(),
                agent: HashSet::new(),
                now: 1_000_000,
                rng: 99,
                strokes: 0,
            }
        }

        fn people_set(&self) -> HashSet<usize> {
            let mut set: HashSet<usize> = self
                .people
                .iter()
                .filter(|(_, at)| **at + crate::state::HUMAN_MEMORY_MS > self.now)
                .map(|(i, _)| *i)
                .collect();
            for &(x, y, w, h) in &self.keep {
                for ty in y..y + h {
                    for tx in x..x + w {
                        set.insert(self.rules.index(tx, ty));
                    }
                }
            }
            set
        }

        fn person_paints(&mut self, x: u32, y: u32) {
            let i = self.rules.index(x, y);
            self.pixels[i] = 5;
            self.people.insert(i, self.now);
        }

        fn rand(&mut self, n: u64) -> u64 {
            self.rng = self
                .rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.rng >> 33) % n
        }

        /// One tick of `step_ms`; every stroke commits. Checks that no
        /// stroke paints a protected tile, then applies it.
        fn tick(&mut self, d: &mut Director, step_ms: u64, busy: bool) -> Vec<Stroke> {
            self.now += step_ms;
            let people = self.people_set();
            let strokes = {
                let view = View {
                    rules: &self.rules,
                    people: &people,
                    agent_paint: &self.agent,
                    pixels: &self.pixels,
                    ceiling_busy: busy,
                };
                d.tick(self.now, &view)
            };
            for s in &strokes {
                let i = self.rules.index(s.tile.x, s.tile.y);
                assert!(
                    !people.contains(&i),
                    "{} painted over a person at {:?}",
                    s.name,
                    s.tile
                );
                assert!(s.branch.starts_with("agent/"), "{}", s.branch);
                self.pixels[i] = s.tile.color;
                self.agent.insert(i);
                self.strokes += 1;
                d.finished(s.persona, true, self.now);
            }
            strokes
        }
    }

    fn director(settings: PersonaSettings) -> Director {
        Director::new(ROSTER, settings, 42, 1_000_000).unwrap()
    }

    /// Over a simulated six hours with people painting all over: personas
    /// never paint a person's recent tile or a keep area, 3 to 6 are always
    /// on, who is on changes, pushes stay near 1 to 3 a second, and every
    /// finished picture has every tile on the canvas in its colour.
    #[test]
    fn six_simulated_hours_keep_every_invariant() {
        let mut w = World::new(64, 64);
        w.keep = vec![(0, 0, 20, 12)];
        let mut d = director(PersonaSettings::default());
        let (mut lo, mut hi) = (usize::MAX, 0);
        let mut seen = HashSet::new();
        let mut changes = 0;
        let mut last: Vec<usize> = Vec::new();
        let mut complete = 0;
        let mut pictures = HashSet::new();
        let mut per_minute = Vec::new();
        let mut minute = 0;
        let step = 100;
        for t in 0..(6 * 3600 * 1000 / step) {
            // About once a minute a person paints a short stroke of up
            // to 8 tiles around a random spot.
            if w.rand(600) == 0 {
                let (x, y) = (w.rand(60) as u32, w.rand(60) as u32);
                for _ in 0..1 + w.rand(8) {
                    let (dx, dy) = (w.rand(4) as u32, w.rand(4) as u32);
                    w.person_paints(x + dx, y + dy);
                }
            }
            let strokes = w.tick(&mut d, step, false);
            minute += strokes.len();
            if (t + 1) % 600 == 0 {
                per_minute.push(minute);
                minute = 0;
            }
            let active: Vec<usize> = d.active().iter().map(|(i, _)| *i).collect();
            lo = lo.min(active.len());
            hi = hi.max(active.len());
            seen.extend(active.iter().copied());
            if active != last {
                changes += 1;
                last = active;
            }
            for c in d.take_completed() {
                complete += 1;
                pictures.insert(c.picture);
                // Finished means every tile is on the canvas in its colour,
                // none of them a person's.
                for t in &c.tiles {
                    assert_eq!(
                        w.pixels[w.rules.index(t.x, t.y)],
                        t.color,
                        "{} finished with ({}, {}) not painted",
                        c.picture,
                        t.x,
                        t.y
                    );
                }
                let people = w.people_set();
                assert!(
                    !c.tiles
                        .iter()
                        .any(|t| people.contains(&w.rules.index(t.x, t.y))),
                    "{} finished on a person's tile",
                    c.picture
                );
            }
        }
        eprintln!(
            "6 h: on {lo}..={hi}, {} personas came, line-up changed {changes} times, \
             {complete} pictures finished, {} relocations, {:.2} pushes/s, busiest minute {}",
            seen.len(),
            d.relocations,
            w.strokes as f64 / (6.0 * 3600.0),
            per_minute.iter().max().unwrap()
        );
        assert!(lo >= 3, "fewer than 3 on: {lo}");
        assert!(hi <= 6, "more than 6 on: {hi}");
        assert!(hi >= 5, "never more than {hi} on");
        assert!(seen.len() >= 10, "only {} personas came", seen.len());
        assert!(changes >= 20, "the line-up changed {changes} times");
        assert!(complete >= 100, "{complete} pictures finished");
        let pps = w.strokes as f64 / (6.0 * 3600.0);
        assert!((0.5..=3.0).contains(&pps), "{pps} pushes/s");
        // Never above the budget in any minute (3/s plus one burst).
        assert!(
            per_minute.iter().all(|m| *m <= 3 * 60 + 3),
            "{per_minute:?}"
        );
        assert!(d.relocations > 0, "people never got in the way");
        // The demo's must-haves are each painted whole at least once.
        for must in ["california-flag", "swedish-flag", "house", "frog"] {
            assert!(
                pictures.contains(must),
                "{must} never finished: {pictures:?}"
            );
        }
    }

    /// The budget holds even when many personas want to push at once.
    #[test]
    fn total_pushes_stay_within_the_budget() {
        let mut w = World::new(128, 128);
        let mut d = director(PersonaSettings {
            min_active: 13,
            max_active: 13,
            push_gap_ms: (1000, 1000),
            pause_percent: 0,
            pushes_per_second: 2.0,
            ..PersonaSettings::default()
        });
        let mut n = 0;
        for _ in 0..600 {
            n += w.tick(&mut d, 100, false).len();
        }
        // 60 s at 2/s, plus the one push banked at the start.
        assert!(n <= 121, "{n}");
        assert!(n >= 100, "{n}");
    }

    /// When a person paints inside a picture being painted, the persona
    /// starts it again elsewhere and never touches the person's tile.
    #[test]
    fn a_taken_spot_moves_the_picture() {
        let mut w = World::new(48, 48);
        let mut d = director(PersonaSettings {
            min_active: 1,
            max_active: 1,
            pause_percent: 0,
            ..PersonaSettings::default()
        });
        // Paint until a picture is under way.
        while d.pictures().is_empty() || w.strokes == 0 {
            w.tick(&mut d, 100, false);
        }
        // A person paints one of its tiles that is not painted yet.
        let job = d
            .seats
            .iter()
            .find_map(|s| s.shift.as_ref()?.job.as_ref())
            .unwrap();
        let tile = *job.tiles.last().unwrap();
        w.person_paints(tile.x, tile.y);
        let taken = w.rules.index(tile.x, tile.y);
        // Its next turn moves it (`World::tick` also checks that no stroke
        // paints a person's tile).
        for _ in 0..100 {
            if d.relocations > 0 {
                break;
            }
            w.tick(&mut d, 100, false);
        }
        assert_eq!(d.relocations, 1);
        let moved = d
            .seats
            .iter()
            .find_map(|s| s.shift.as_ref()?.job.as_ref())
            .expect("the picture moved, not dropped");
        assert!(moved.tiles.iter().all(|t| w.rules.index(t.x, t.y) != taken));
        for _ in 0..600 {
            w.tick(&mut d, 100, false);
        }
        assert_eq!(d.relocations, 1, "nobody painted into it again");
    }

    /// No spot clear of people: nobody paints at all.
    #[test]
    fn a_canvas_full_of_peoples_work_is_left_alone() {
        let mut w = World::new(24, 24);
        for y in 0..24 {
            for x in 0..24 {
                w.person_paints(x, y);
            }
        }
        let mut d = director(PersonaSettings::default());
        for _ in 0..3000 {
            assert!(w.tick(&mut d, 100, false).is_empty());
        }
        assert!(d.active().len() >= 3);
    }

    /// At the server-wide ceiling nobody pushes; afterwards they carry on.
    #[test]
    fn personas_back_off_at_the_ceiling() {
        let mut w = World::new(64, 64);
        let mut d = director(PersonaSettings::default());
        let mut before = 0;
        for _ in 0..600 {
            before += w.tick(&mut d, 100, false).len();
        }
        assert!(before > 0);
        for _ in 0..600 {
            assert!(
                w.tick(&mut d, 100, true).is_empty(),
                "pushed at the ceiling"
            );
        }
        // Released: everyone waits out its backoff (at most 2 x 5 s), then
        // paints again.
        let mut after = 0;
        for _ in 0..300 {
            after += w.tick(&mut d, 100, false).len();
        }
        assert!(after > 0);
    }

    /// One push carries one tile, a persona has one push in flight, and a
    /// push that failed is retried with the same tile.
    #[test]
    fn one_tile_per_push_and_a_failed_push_is_retried() {
        let mut w = World::new(32, 32);
        let mut d = director(PersonaSettings {
            min_active: 1,
            max_active: 1,
            pause_percent: 0,
            push_gap_ms: (1000, 1000),
            ..PersonaSettings::default()
        });
        let first = loop {
            w.now += 100;
            let people = w.people_set();
            let view = View {
                rules: &w.rules,
                people: &people,
                agent_paint: &w.agent,
                pixels: &w.pixels,
                ceiling_busy: false,
            };
            let s = d.tick(w.now, &view);
            if let Some(s) = s.first() {
                assert_eq!(s.branch, d.active()[0].1);
                break s.clone();
            }
        };
        // In flight: nothing more from it, however long.
        for _ in 0..50 {
            w.now += 100;
            let people = w.people_set();
            let view = View {
                rules: &w.rules,
                people: &people,
                agent_paint: &w.agent,
                pixels: &w.pixels,
                ceiling_busy: false,
            };
            assert!(d.tick(w.now, &view).is_empty());
        }
        d.finished(first.persona, false, w.now);
        w.now += 1000;
        let people = w.people_set();
        let view = View {
            rules: &w.rules,
            people: &people,
            agent_paint: &w.agent,
            pixels: &w.pixels,
            ceiling_busy: false,
        };
        let again = d.tick(w.now, &view);
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].tile, first.tile, "the failed tile comes again");
        assert_eq!(again[0].author, ROSTER[first.persona].author());
    }

    #[test]
    fn repeated_failures_back_off_but_never_exhaust_retries() {
        let mut w = World::new(32, 32);
        let mut d = director(PersonaSettings {
            min_active: 1,
            max_active: 1,
            pause_percent: 0,
            push_gap_ms: (100, 100),
            ..PersonaSettings::default()
        });
        let mut first = None;
        let mut previous_attempt = 0;
        let mut attempts = 0;
        let mut recovered = false;
        for _ in 0..120_000 {
            w.now += 100;
            let people = w.people_set();
            let view = View {
                rules: &w.rules,
                people: &people,
                agent_paint: &w.agent,
                pixels: &w.pixels,
                ceiling_busy: false,
            };
            for stroke in d.tick(w.now, &view) {
                if let Some(tile) = first {
                    assert_eq!(stroke.tile, tile, "failed tile must not be lost");
                    let delay = w.now - previous_attempt;
                    let minimum = (1000u64 << (attempts - 1).min(6)).min(60_000);
                    assert!(
                        delay >= minimum,
                        "attempt {attempts}: retried after {delay} ms"
                    );
                    assert!(delay <= 60_100, "retry must not be parked forever");
                } else {
                    first = Some(stroke.tile);
                }
                attempts += 1;
                previous_attempt = w.now;
                d.finished(stroke.persona, attempts > 100, w.now);
                if attempts > 100 {
                    recovered = true;
                    break;
                }
            }
            if recovered {
                break;
            }
        }
        assert!(recovered, "retries stopped after {attempts} attempts");
        let before = w.strokes;
        for _ in 0..100 {
            w.tick(&mut d, 100, false);
        }
        assert!(w.strokes > before, "painting resumes after recovery");
    }

    #[test]
    fn personas_keep_painting_for_ninety_days_without_a_session_budget() {
        let mut w = World::new(64, 64);
        let mut d = director(PersonaSettings::default());
        let mut previous = 0;
        for day in 0..90 {
            for _ in 0..24 * 60 {
                w.tick(&mut d, 60_000, false);
                assert!((3..=6).contains(&d.active().len()));
            }
            assert!(w.strokes > previous + 100, "painting stopped on day {day}");
            previous = w.strokes;
        }
    }

    /// On a canvas barely larger than the widest picture, placing near a
    /// finished picture and moving one stays on the canvas (no underflow).
    #[test]
    fn a_small_canvas_keeps_every_picture_on_it() {
        for size in [16, 17, 20, 30] {
            let mut w = World::new(size, size);
            let mut d = director(PersonaSettings {
                min_active: 2,
                max_active: 3,
                ..PersonaSettings::default()
            });
            for k in 0..20_000u64 {
                if k % 997 == 0 {
                    let (x, y) = (
                        w.rand(u64::from(size)) as u32,
                        w.rand(u64::from(size)) as u32,
                    );
                    w.person_paints(x, y);
                }
                for s in w.tick(&mut d, 100, false) {
                    assert!(s.tile.x < size && s.tile.y < size, "{s:?}");
                }
            }
            assert!(w.strokes > 0, "{size}: nothing painted");
            let (x, y, pw, ph) = (0, 0, 16, 16);
            let mut rng = Director::new(ROSTER, PersonaSettings::default(), 7, 0).unwrap();
            let people = HashSet::new();
            let agent = HashSet::new();
            let view = View {
                rules: &w.rules,
                people: &people,
                agent_paint: &agent,
                pixels: &w.pixels,
                ceiling_busy: false,
            };
            for _ in 0..200 {
                let job = rng
                    .choose_spot(&view, &pictures::CALIFORNIA, 0, 0, Some((x, y, pw, ph)))
                    .expect("a 16-wide picture fits");
                let (jx, jy, jw, jh) = job.rect;
                assert!(jx + jw <= size && jy + jh <= size, "{size}: {:?}", job.rect);
            }
        }
    }

    #[test]
    fn settings_are_checked() {
        let ok = PersonaSettings::default();
        assert!(ok.validate(ROSTER.len()).is_ok());
        for bad in [
            PersonaSettings {
                min_active: 0,
                ..ok.clone()
            },
            PersonaSettings {
                min_active: 7,
                ..ok.clone()
            },
            PersonaSettings {
                max_active: 99,
                ..ok.clone()
            },
            PersonaSettings {
                pushes_per_second: 0.0,
                ..ok.clone()
            },
            PersonaSettings {
                push_gap_ms: (5, 1),
                ..ok.clone()
            },
        ] {
            assert!(bad.validate(ROSTER.len()).is_err(), "{bad:?}");
        }
    }
}
