//! The tile format and push rules of `demo/place/docs/TILES.md`, as the
//! pre-receive check runs them. This is the Rust twin of
//! `demo/place/src/shared/tiles.ts`: both must accept and reject exactly
//! the same inputs, and the tests below port that file's cases.

use serde::Serialize;

/// The trunk branch; only the merger bot writes it.
pub const TRUNK_REF: &str = "refs/heads/main";

/// Branch namespaces the server writes for its own painters (web visitors,
/// ambient agents, the swarm). A Git client may not push into them, so a
/// visitor's branch only ever moves through that visitor's session.
pub const RESERVED_PREFIXES: [&str; 3] = ["visitors/", "agent/", "swarm/"];

/// Branches under this prefix (after `refs/heads/`) only move forward
/// for Git clients: they may be created and fast-forwarded, never deleted
/// or rewritten, because anyone may push to any of them.
pub const FORWARD_ONLY_PREFIX: &str = "paint/";

/// Whether a Git client's update of `name` must be a creation or a
/// fast-forward ([`FORWARD_ONLY_PREFIX`]).
pub fn forward_only(name: &str) -> bool {
    name.strip_prefix("refs/heads/")
        .is_some_and(|short| short.starts_with(FORWARD_ONLY_PREFIX))
}

/// The only file mode a tile may have.
pub const TILE_MODE: u32 = 0o100644;

/// A tile file is exactly this many bytes: `#rrggbb\n`.
pub const TILE_FILE_BYTES: usize = 8;

/// The longest branch name after `refs/heads/`.
pub const MAX_BRANCH_CHARS: usize = 100;

/// One palette color.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PaletteEntry {
    /// Lowercase `#rrggbb`, exactly as in a tile file.
    pub color: String,
    /// A name for people.
    pub name: String,
}

/// The default palette, the same as `src/shared/palette.ts`.
pub fn default_palette() -> Vec<PaletteEntry> {
    [
        ("#ffffff", "White"),
        ("#d4d7d9", "Light grey"),
        ("#898d90", "Grey"),
        ("#1b1f22", "Black"),
        ("#ff99aa", "Pink"),
        ("#d6262b", "Red"),
        ("#ff7a1a", "Orange"),
        ("#9c6926", "Brown"),
        ("#ffd635", "Yellow"),
        ("#7eed56", "Lime"),
        ("#00a368", "Green"),
        ("#51e9f4", "Cyan"),
        ("#3690ea", "Blue"),
        ("#2f3ab4", "Navy"),
        ("#b44ac0", "Magenta"),
        ("#6d3a8c", "Purple"),
    ]
    .into_iter()
    .map(|(c, n)| PaletteEntry {
        color: c.to_owned(),
        name: n.to_owned(),
    })
    .collect()
}

/// At most 36 colors fit the one-character-per-pixel snapshot encoding.
pub const MAX_PALETTE: usize = 36;

/// The canvas and its limits.
#[derive(Debug, Clone)]
pub struct Rules {
    /// Tiles across.
    pub width: u32,
    /// Tiles down.
    pub height: u32,
    /// The colors a tile may have; the index is the snapshot encoding.
    pub palette: Vec<PaletteEntry>,
    /// The most distinct tiles one push may change.
    pub tiles_per_push: usize,
}

impl Rules {
    /// The palette index of `color`, if it is in the palette.
    pub fn color_index(&self, color: &str) -> Option<u8> {
        self.palette
            .iter()
            .position(|p| p.color == color)
            .map(|i| i as u8)
    }

    /// Row-major index of a tile.
    pub fn index(&self, x: u32, y: u32) -> usize {
        y as usize * self.width as usize + x as usize
    }

    /// Number of tiles.
    pub fn len(&self) -> usize {
        self.width as usize * self.height as usize
    }

    /// Whether the canvas has no tiles (never, for a valid configuration).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A painted tile, by palette index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Tile {
    /// Column.
    pub x: u32,
    /// Row.
    pub y: u32,
    /// Palette index.
    pub color: u8,
}

/// Why a push or a stroke is refused: the codes of `api/openapi.yaml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    /// A changed path outside `tiles/`.
    OutsideTiles,
    /// A path in `tiles/` that is not `tiles/<x>_<y>.txt`.
    BadTilePath,
    /// A tile outside the canvas.
    OutOfBounds,
    /// A tile that is not a regular non-executable file.
    BadMode,
    /// A deleted tile.
    DeleteNotAllowed,
    /// Tile contents not exactly `#rrggbb\n`.
    BadFormat,
    /// A color not in the palette.
    ColorNotInPalette,
    /// More distinct tiles than the push limit.
    TooManyTiles,
    /// A stroke naming one tile twice.
    DuplicateTile,
    /// A stroke with no tiles.
    EmptyStroke,
    /// A merge commit.
    MergeCommit,
    /// A push to main, or into a server-owned namespace.
    ProtectedRef,
    /// A ref that is not a branch, or a branch name that is too long.
    BadRef,
}

impl Code {
    /// The code as the API spells it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OutsideTiles => "outside-tiles",
            Self::BadTilePath => "bad-tile-path",
            Self::OutOfBounds => "out-of-bounds",
            Self::BadMode => "bad-mode",
            Self::DeleteNotAllowed => "delete-not-allowed",
            Self::BadFormat => "bad-format",
            Self::ColorNotInPalette => "color-not-in-palette",
            Self::TooManyTiles => "too-many-tiles",
            Self::DuplicateTile => "duplicate-tile",
            Self::EmptyStroke => "empty-stroke",
            Self::MergeCommit => "merge-commit",
            Self::ProtectedRef => "protected-ref",
            Self::BadRef => "bad-ref",
        }
    }
}

impl Serialize for Code {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

/// One problem, written for a person reading `git push` output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Rejection {
    /// Machine-readable reason.
    pub code: Code,
    /// The file, commit or ref the problem is about.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// One line.
    pub message: String,
}

fn reject(code: Code, path: &str, message: String) -> Rejection {
    Rejection {
        code,
        path: Some(path.to_owned()),
        message,
    }
}

/// The repository path of a tile, `tiles/<x>_<y>.txt`.
pub fn tile_path(x: u32, y: u32) -> String {
    format!("tiles/{x}_{y}.txt")
}

/// The exact bytes of a tile file.
pub fn tile_bytes(color: &str) -> Vec<u8> {
    format!("{color}\n").into_bytes()
}

/// Parses a path into tile coordinates.
pub fn parse_tile_path(path: &str, rules: &Rules) -> Result<(u32, u32), Rejection> {
    let Some(name) = path.strip_prefix("tiles/") else {
        return Err(reject(
            Code::OutsideTiles,
            path,
            format!("{path}: only files under tiles/ may change"),
        ));
    };
    let bad = || {
        reject(
            Code::BadTilePath,
            path,
            format!(
                "{path}: tile files are named tiles/<x>_<y>.txt with plain decimal coordinates, e.g. tiles/12_40.txt"
            ),
        )
    };
    let stem = name.strip_suffix(".txt").ok_or_else(bad)?;
    let (x, y) = stem.split_once('_').ok_or_else(bad)?;
    let (x, y) = (canonical(x).ok_or_else(bad)?, canonical(y).ok_or_else(bad)?);
    if x >= rules.width || y >= rules.height {
        return Err(reject(
            Code::OutOfBounds,
            path,
            format!(
                "{path}: ({x}, {y}) is outside the {w}x{h} canvas; x is 0-{mx}, y is 0-{my}",
                w = rules.width,
                h = rules.height,
                mx = rules.width - 1,
                my = rules.height - 1
            ),
        ));
    }
    Ok((x, y))
}

/// `0`, or 1 to 5 digits without a leading zero.
fn canonical(s: &str) -> Option<u32> {
    let ok = !s.is_empty()
        && s.len() <= 5
        && s.bytes().all(|b| b.is_ascii_digit())
        && (s == "0" || !s.starts_with('0'));
    ok.then(|| s.parse().ok()).flatten()
}

/// Parses a tile file's contents into a palette index.
pub fn parse_tile_content(content: &[u8], rules: &Rules, path: &str) -> Result<u8, Rejection> {
    let well_formed = content.len() == TILE_FILE_BYTES
        && content[0] == b'#'
        && content[1..7]
            .iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        && content[7] == b'\n';
    if !well_formed {
        return Err(reject(
            Code::BadFormat,
            path,
            format!("{path}: {}", describe_format_problem(content)),
        ));
    }
    let color = std::str::from_utf8(&content[..7]).expect("checked ASCII");
    rules.color_index(color).ok_or_else(|| {
        let all: Vec<&str> = rules.palette.iter().map(|p| p.color.as_str()).collect();
        reject(
            Code::ColorNotInPalette,
            path,
            format!(
                "{path}: {color} is not in the palette; pick one of {}",
                all.join(" ")
            ),
        )
    })
}

fn describe_format_problem(text: &[u8]) -> String {
    let expected = r##"expected exactly "#rrggbb" and one newline, e.g. "#d6262b\n""##;
    let hex6 = |s: &[u8]| s.len() == 6 && s.iter().all(u8::is_ascii_hexdigit);
    let hint = if text.is_empty() {
        "the file is empty; "
    } else if text.starts_with(b"\xef\xbb\xbf") {
        "remove the byte-order mark; "
    } else if text.contains(&b'\r') {
        "use LF line endings, not CRLF; "
    } else if text.len() == 7 && text[0] == b'#' && hex6(&text[1..]) {
        "end the file with one newline; "
    } else if text.len() == 8 && text[0] == b'#' && hex6(&text[1..7]) && text[7] == b'\n' {
        "use lowercase hex digits; "
    } else if hex6(text) || (text.len() == 7 && hex6(&text[..6]) && text[6] == b'\n') {
        r##"start the color with "#"; "##
    } else if text.len() > TILE_FILE_BYTES {
        return format!(
            "the file has extra content ({} bytes); {expected}",
            text.len()
        );
    } else {
        ""
    };
    format!("{hint}{expected}")
}

/// Who is pushing, for the ref rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pusher {
    /// A Git client.
    Git,
    /// The server itself (web visitors, agents, the merger bot).
    Server,
}

/// Checks the ref of an update. Git clients may create, update or delete
/// any branch except trunk and the server's own namespaces; tags and other
/// refs are refused.
pub fn validate_ref(name: &str, pusher: Pusher) -> Option<Rejection> {
    if name == TRUNK_REF {
        return Some(reject(
            Code::ProtectedRef,
            name,
            format!(
                "{name}: main is written by the merger bot; push to your own branch, e.g. git push origin HEAD:refs/heads/paint/<you>"
            ),
        ));
    }
    let Some(short) = name.strip_prefix("refs/heads/") else {
        return Some(bad_ref(name));
    };
    // Counted in UTF-16 units, as `tiles.ts` counts them.
    if short.encode_utf16().count() > MAX_BRANCH_CHARS {
        return Some(bad_ref(name));
    }
    if pusher == Pusher::Git && RESERVED_PREFIXES.iter().any(|p| short.starts_with(p)) {
        return Some(reject(
            Code::ProtectedRef,
            name,
            format!(
                "{name}: branches under {} belong to the web page and its agents; push to your own branch, e.g. refs/heads/paint/<you>",
                RESERVED_PREFIXES.join(", ")
            ),
        ));
    }
    None
}

fn bad_ref(name: &str) -> Rejection {
    reject(
        Code::BadRef,
        name,
        format!(
            "{name}: only branches (refs/heads/*, at most {MAX_BRANCH_CHARS} characters) can be pushed"
        ),
    )
}

/// What a commit did to one file, against its first parent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeKind {
    /// A new file.
    Add,
    /// A changed file (contents or mode).
    Modify,
    /// A removed file.
    Delete,
}

/// One file change, as the pre-receive check sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChange {
    /// Full path, `/`-separated.
    pub path: String,
    /// Added, modified or deleted.
    pub kind: ChangeKind,
    /// The new mode; `None` for a delete.
    pub mode: Option<u32>,
    /// The new contents, when the check needs them (a valid tile path with
    /// mode 100644); `None` otherwise.
    pub content: Option<Vec<u8>>,
}

/// One commit of a push, oldest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushedCommit {
    /// Hex commit ID.
    pub id: String,
    /// Number of parents.
    pub parent_count: usize,
    /// Changes against the first parent.
    pub changes: Vec<FileChange>,
}

/// The pre-receive rules over the commits a push adds to one branch
/// (oldest first). Returns the final color of every tile the push paints
/// (the last write wins), or every problem found.
pub fn validate_push(commits: &[PushedCommit], rules: &Rules) -> Result<Vec<Tile>, Vec<Rejection>> {
    let mut rejections = Vec::new();
    let mut tiles: std::collections::BTreeMap<(u32, u32), u8> = Default::default();
    for commit in commits {
        if commit.parent_count > 1 {
            let short = &commit.id[..commit.id.len().min(12)];
            rejections.push(reject(
                Code::MergeCommit,
                &commit.id,
                format!("{short}: merge commits are not accepted; rebase onto origin/main instead"),
            ));
            continue;
        }
        for change in &commit.changes {
            match validate_change(change, rules) {
                Ok(t) => {
                    tiles.insert((t.x, t.y), t.color);
                }
                Err(r) => rejections.push(r),
            }
        }
    }
    if tiles.len() > rules.tiles_per_push {
        rejections.push(Rejection {
            code: Code::TooManyTiles,
            path: None,
            message: format!(
                "this push changes {} tiles; the limit is {} per push",
                tiles.len(),
                rules.tiles_per_push
            ),
        });
    }
    if rejections.is_empty() {
        Ok(tiles
            .into_iter()
            .map(|((x, y), color)| Tile { x, y, color })
            .collect())
    } else {
        Err(rejections)
    }
}

fn validate_change(change: &FileChange, rules: &Rules) -> Result<Tile, Rejection> {
    let path = &change.path;
    let (x, y) = parse_tile_path(path, rules)?;
    if change.kind == ChangeKind::Delete {
        return Err(reject(
            Code::DeleteNotAllowed,
            path,
            format!("{path}: tiles can be repainted but not deleted; paint it #ffffff instead"),
        ));
    }
    if change.mode != Some(TILE_MODE) {
        let mode = change
            .mode
            .map_or_else(|| "unknown".to_owned(), |m| format!("{m:o}"));
        return Err(reject(
            Code::BadMode,
            path,
            format!("{path}: tiles must be regular files (mode 100644), not mode {mode}"),
        ));
    }
    let color = parse_tile_content(change.content.as_deref().unwrap_or_default(), rules, path)?;
    Ok(Tile { x, y, color })
}

/// One tile of a web stroke, as sent: the API checks types (a 400) before
/// the rules (a 422).
#[derive(Debug, Clone, PartialEq)]
pub struct StrokeTile {
    /// Column, as a JSON number.
    pub x: f64,
    /// Row, as a JSON number.
    pub y: f64,
    /// Color as sent.
    pub color: String,
}

/// The rules for a stroke painted through the web API: one commit, the
/// same limits as a push, and no tile twice.
pub fn validate_stroke(tiles: &[StrokeTile], rules: &Rules) -> Result<Vec<Tile>, Vec<Rejection>> {
    let mut rejections = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut ok = Vec::new();
    for t in tiles {
        let as_u32 = |v: f64| {
            (v.fract() == 0.0 && (0.0..=f64::from(u32::MAX)).contains(&v)).then_some(v as u32)
        };
        let path = match (as_u32(t.x), as_u32(t.y)) {
            (Some(x), Some(y)) => tile_path(x, y),
            _ => format!("tiles/{}_{}.txt", js_number(t.x), js_number(t.y)),
        };
        let (x, y) = match parse_tile_path(&path, rules) {
            Ok(xy) => xy,
            Err(r) => {
                rejections.push(r);
                continue;
            }
        };
        let color = match parse_tile_content(&tile_bytes(&t.color), rules, &path) {
            Ok(c) => c,
            Err(r) => {
                rejections.push(r);
                continue;
            }
        };
        if !seen.insert((x, y)) {
            rejections.push(reject(
                Code::DuplicateTile,
                &path,
                format!("{path}: painted twice in one stroke"),
            ));
            continue;
        }
        ok.push(Tile { x, y, color });
    }
    if tiles.is_empty() {
        rejections.push(Rejection {
            code: Code::EmptyStroke,
            path: None,
            message: "a stroke must paint at least one tile".to_owned(),
        });
    }
    if tiles.len() > rules.tiles_per_push {
        rejections.push(Rejection {
            code: Code::TooManyTiles,
            path: None,
            message: format!(
                "this stroke paints {} tiles; the limit is {} per push",
                tiles.len(),
                rules.tiles_per_push
            ),
        });
    }
    if rejections.is_empty() {
        Ok(ok)
    } else {
        Err(rejections)
    }
}

/// A number as JavaScript prints it in a template string (`1.5`, `-1`).
fn js_number(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e21 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

#[cfg(test)]
mod tests {
    //! The cases of `demo/place/test/tiles.test.ts`, plus the reserved
    //! namespaces only the server checks.

    use super::*;

    fn rules() -> Rules {
        Rules {
            width: 64,
            height: 48,
            palette: default_palette(),
            tiles_per_push: 4,
        }
    }

    const RED: &str = "#d6262b";

    fn add(path: &str, content: &[u8], mode: u32) -> FileChange {
        FileChange {
            path: path.to_owned(),
            kind: ChangeKind::Add,
            mode: Some(mode),
            content: Some(content.to_vec()),
        }
    }

    fn codes(r: Result<Vec<Tile>, Vec<Rejection>>) -> Vec<&'static str> {
        match r {
            Ok(_) => vec![],
            Err(rs) => rs.into_iter().map(|r| r.code.as_str()).collect(),
        }
    }

    #[test]
    fn tile_paths_round_trip_every_corner() {
        for (x, y) in [(0, 0), (63, 0), (0, 47), (63, 47), (12, 40)] {
            assert_eq!(parse_tile_path(&tile_path(x, y), &rules()), Ok((x, y)));
        }
    }

    #[test]
    fn bad_tile_paths_are_refused_with_their_code() {
        for (path, code) in [
            ("README.md", "outside-tiles"),
            ("tile/1_2.txt", "outside-tiles"),
            ("./tiles/1_2.txt", "outside-tiles"),
            ("tiles/01_2.txt", "bad-tile-path"),
            ("tiles/1_02.txt", "bad-tile-path"),
            ("tiles/-1_2.txt", "bad-tile-path"),
            ("tiles/1_2.TXT", "bad-tile-path"),
            ("tiles/1_2.txt.bak", "bad-tile-path"),
            ("tiles/1-2.txt", "bad-tile-path"),
            ("tiles/sub/1_2.txt", "bad-tile-path"),
            ("tiles/1_2", "bad-tile-path"),
            ("tiles/ 1_2.txt", "bad-tile-path"),
            ("tiles/1e1_2.txt", "bad-tile-path"),
            ("tiles/123456_2.txt", "bad-tile-path"),
            ("tiles/1_2_3.txt", "bad-tile-path"),
            ("tiles/64_0.txt", "out-of-bounds"),
            ("tiles/0_48.txt", "out-of-bounds"),
        ] {
            let r = parse_tile_path(path, &rules()).unwrap_err();
            assert_eq!((path, r.code.as_str()), (path, code));
            assert_eq!(r.path.as_deref(), Some(path));
        }
    }

    #[test]
    fn tile_contents_are_exactly_a_palette_color_and_one_newline() {
        assert_eq!(tile_bytes(RED), b"#d6262b\n");
        for (i, p) in default_palette().iter().enumerate() {
            assert_eq!(
                parse_tile_content(&tile_bytes(&p.color), &rules(), "t"),
                Ok(i as u8)
            );
        }
        for (content, hint) in [
            (&b""[..], "empty"),
            (b"#d6262b", "newline"),
            (b"#D6262B\n", "lowercase"),
            (b"d6262b\n", "\"#\""),
            (b"#d6262b\r\n", "CRLF"),
            (b"\xef\xbb\xbf#d6262b\n", "byte-order mark"),
            (b"#d6262b\n\n", "extra content"),
            (b"#d6262b \n", "expected exactly"),
            (b" #d6262b\n", "expected exactly"),
            (b"#d626\n", "expected exactly"),
            (b"#d6262bff\n", "extra content"),
            (b"red\n", "expected exactly"),
        ] {
            let r = parse_tile_content(content, &rules(), "tiles/1_1.txt").unwrap_err();
            assert_eq!(r.code, Code::BadFormat, "{content:?}");
            assert!(r.message.contains(hint), "{content:?}: {}", r.message);
            assert!(r.message.starts_with("tiles/1_1.txt: "));
        }
        let r = parse_tile_content(b"#123456\n", &rules(), "t").unwrap_err();
        assert_eq!(r.code, Code::ColorNotInPalette);
    }

    #[test]
    fn pushes_accept_additions_and_modifications_last_write_wins() {
        let v = validate_push(
            &[
                PushedCommit {
                    id: "a".repeat(40),
                    parent_count: 1,
                    changes: vec![
                        add("tiles/1_1.txt", b"#d6262b\n", TILE_MODE),
                        add("tiles/2_1.txt", b"#3690ea\n", TILE_MODE),
                    ],
                },
                PushedCommit {
                    id: "b".repeat(40),
                    parent_count: 1,
                    changes: vec![FileChange {
                        kind: ChangeKind::Modify,
                        ..add("tiles/1_1.txt", b"#ffd635\n", TILE_MODE)
                    }],
                },
            ],
            &rules(),
        )
        .unwrap();
        let r = rules();
        assert_eq!(
            v,
            vec![
                Tile {
                    x: 1,
                    y: 1,
                    color: r.color_index("#ffd635").unwrap()
                },
                Tile {
                    x: 2,
                    y: 1,
                    color: r.color_index("#3690ea").unwrap()
                },
            ]
        );
    }

    #[test]
    fn every_rejection_is_collected_not_just_the_first() {
        let v = validate_push(
            &[
                PushedCommit {
                    id: "c".repeat(40),
                    parent_count: 1,
                    changes: vec![
                        add("README.md", b"hi\n", TILE_MODE),
                        add("tiles/1_1.txt", b"#d6262b\n", 0o100755),
                        add("tiles/1_2.txt", b"#d6262b\n", 0o120000),
                        FileChange {
                            path: "tiles/3_3.txt".into(),
                            kind: ChangeKind::Delete,
                            mode: None,
                            content: None,
                        },
                        add("tiles/4_4.txt", b"#D6262B\n", TILE_MODE),
                    ],
                },
                PushedCommit {
                    id: "d".repeat(40),
                    parent_count: 2,
                    changes: vec![],
                },
            ],
            &rules(),
        );
        assert_eq!(
            codes(v),
            [
                "outside-tiles",
                "bad-mode",
                "bad-mode",
                "delete-not-allowed",
                "bad-format",
                "merge-commit"
            ]
        );
    }

    #[test]
    fn distinct_tiles_per_push_are_limited_across_commits() {
        let commits = |same: bool| -> Vec<PushedCommit> {
            (0..5)
                .map(|i| PushedCommit {
                    id: format!("{i}").repeat(40),
                    parent_count: 1,
                    changes: vec![add(
                        &tile_path(if same { 0 } else { i }, 0),
                        b"#d6262b\n",
                        TILE_MODE,
                    )],
                })
                .collect()
        };
        assert_eq!(
            codes(validate_push(&commits(false), &rules())),
            ["too-many-tiles"]
        );
        // Repainting one tile many times counts once.
        assert!(validate_push(&commits(true), &rules()).is_ok());
    }

    #[test]
    fn a_rename_is_a_delete_plus_an_add_and_is_refused() {
        let v = validate_push(
            &[PushedCommit {
                id: "e".repeat(40),
                parent_count: 1,
                changes: vec![
                    FileChange {
                        path: "tiles/1_1.txt".into(),
                        kind: ChangeKind::Delete,
                        mode: None,
                        content: None,
                    },
                    add("tiles/2_2.txt", b"#d6262b\n", TILE_MODE),
                ],
            }],
            &rules(),
        );
        assert_eq!(codes(v), ["delete-not-allowed"]);
    }

    #[test]
    fn main_and_server_namespaces_are_protected_and_only_branches_pass() {
        let code = |n: &str, p| validate_ref(n, p).map(|r| r.code.as_str());
        assert_eq!(code("refs/heads/main", Pusher::Git), Some("protected-ref"));
        assert_eq!(
            code("refs/heads/main", Pusher::Server),
            Some("protected-ref")
        );
        assert_eq!(code("refs/tags/v1", Pusher::Git), Some("bad-ref"));
        assert_eq!(
            code(&format!("refs/heads/{}", "x".repeat(101)), Pusher::Git),
            Some("bad-ref")
        );
        assert_eq!(
            code(&format!("refs/heads/{}", "x".repeat(100)), Pusher::Git),
            None
        );
        assert_eq!(code("refs/heads/paint/ada", Pusher::Git), None);
        for reserved in ["visitors/k3x9", "agent/moss-12", "swarm/agent-0001"] {
            let name = format!("refs/heads/{reserved}");
            assert_eq!(code(&name, Pusher::Git), Some("protected-ref"), "{name}");
            assert_eq!(code(&name, Pusher::Server), None, "{name}");
        }
        // Only the exact prefixes are reserved.
        assert_eq!(code("refs/heads/visitorsx/a", Pusher::Git), None);
    }

    #[test]
    fn strokes_are_checked_like_one_commit_and_refuse_duplicates() {
        let t = |x: f64, y: f64, c: &str| StrokeTile {
            x,
            y,
            color: c.into(),
        };
        assert_eq!(
            validate_stroke(&[t(1.0, 2.0, RED)], &rules()),
            Ok(vec![Tile {
                x: 1,
                y: 2,
                color: 5
            }])
        );
        let c = |tiles: &[StrokeTile]| codes(validate_stroke(tiles, &rules()));
        assert_eq!(c(&[]), ["empty-stroke"]);
        let five: Vec<_> = (0..5).map(|x| t(f64::from(x), 0.0, RED)).collect();
        assert_eq!(c(&five), ["too-many-tiles"]);
        assert_eq!(c(&[t(1.0, 1.0, RED), t(1.0, 1.0, RED)]), ["duplicate-tile"]);
        assert_eq!(c(&[t(1.5, 1.0, RED)]), ["bad-tile-path"]);
        assert_eq!(c(&[t(-1.0, 1.0, RED)]), ["bad-tile-path"]);
        assert_eq!(c(&[t(99.0, 1.0, RED)]), ["out-of-bounds"]);
        assert_eq!(c(&[t(1.0, 1.0, "#D6262B")]), ["bad-format"]);
        assert_eq!(c(&[t(1.0, 1.0, "#123456")]), ["color-not-in-palette"]);
    }
}
