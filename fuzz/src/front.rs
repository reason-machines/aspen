//! Targets for the HTTP and SSH front ends (issue #139): the smart-HTTP
//! route parser, the SSH command parser, and the readers of gzip request
//! bodies. JWT verification is in [`crate::jwt`].
//!
//! Each target builds most of its inputs from decisions (a request path
//! out of tricky pieces, a command quoted the way Git quotes it, a gzip
//! stream of a known body) and predicts the exact answer with its own
//! model; the rest of its inputs are raw bytes, checked against the
//! properties that hold for any input. The first input byte picks the mode:
//! [`RAW`] modulo 4 is raw bytes.

use std::collections::BTreeSet;
use std::io::{self, Read, Write};

use aspen_core::name::{RefName, RepoName};
use aspen_proto::service::{ProtocolVersion, Service};
use aspen_server::access::{Endpoint, GitRoute, RouteError};
use aspen_server::fuzzing::{
    self, GzipPushError, MAX_SSH_COMMAND_LEN, ReadBodyError, SshCommandError,
};

use crate::Reached;
use crate::input::{Input, Mix};

/// A first byte that makes a front-end target read the rest of its input
/// as raw bytes (any byte equal to it modulo 4 does).
pub const RAW: u8 = 3;

fn pick<'a>(input: &mut Input<'_>, from: &[&'a str]) -> &'a str {
    from[input.below(from.len())]
}

/// A few bytes of the input as text, for a piece no list foresaw.
fn wild(input: &mut Input<'_>, max: usize) -> String {
    String::from_utf8_lossy(input.bytes(max)).into_owned()
}

// ---------------------------------------------------------------------
// git_route
// ---------------------------------------------------------------------

const INFO_REFS: &str = "/info/refs";
const UPLOAD_PACK: &str = "/git-upload-pack";
const RECEIVE_PACK: &str = "/git-receive-pack";

/// The path suffix of an endpoint.
fn suffix(endpoint: Endpoint) -> &'static str {
    match endpoint {
        Endpoint::UploadPackAdvertisement | Endpoint::ReceivePackAdvertisement => INFO_REFS,
        Endpoint::UploadPack => UPLOAD_PACK,
        Endpoint::ReceivePack => RECEIVE_PACK,
    }
}

/// The query an advertisement needs.
fn service_query(endpoint: Endpoint) -> Option<&'static str> {
    match endpoint {
        Endpoint::UploadPackAdvertisement => Some("service=git-upload-pack"),
        Endpoint::ReceivePackAdvertisement => Some("service=git-receive-pack"),
        Endpoint::UploadPack | Endpoint::ReceivePack => None,
    }
}

/// The path a route is served at, with or without `.git`.
fn render(route: &GitRoute, dot_git: bool) -> String {
    let mut path = format!("/{}", route.repo.as_str());
    if dot_git {
        path.push_str(".git");
    }
    if let Some(scope) = &route.scope {
        path.push_str("/-/");
        path.push_str(scope.as_str());
    }
    path.push_str(suffix(route.endpoint));
    path
}

/// The model of routing, written over path segments rather than string
/// splits: the endpoint is the path's suffix (the advertisement's service is
/// the query's first `service=` item), the repository is every segment
/// before the first `-` segment (a repository segment is never `-`, so the
/// first one starts the scope), with one `.git` dropped from its end, and
/// the scope is every segment after it.
fn route_model(path: &str, query: Option<&str>) -> Result<GitRoute, RouteError> {
    let endpoint = if path.ends_with(INFO_REFS) {
        let service = query.and_then(|q| {
            q.split('&')
                .find(|item| item.starts_with("service="))
                .map(|item| &item["service=".len()..])
        });
        match service {
            Some("git-upload-pack") => Endpoint::UploadPackAdvertisement,
            Some("git-receive-pack") => Endpoint::ReceivePackAdvertisement,
            _ => return Err(RouteError::DumbHttp),
        }
    } else if path.ends_with(UPLOAD_PACK) {
        Endpoint::UploadPack
    } else if path.ends_with(RECEIVE_PACK) {
        Endpoint::ReceivePack
    } else {
        return Err(RouteError::NotGit);
    };
    let base = &path[..path.len() - suffix(endpoint).len()];
    let Some(base) = base.strip_prefix('/') else {
        return Err(RouteError::BadName);
    };
    let segments: Vec<&str> = base.split('/').collect();
    // The scope starts after a `-` segment that has a segment after it
    // (`a/-` has no scope: its last segment is part of the repository).
    let dash = segments[..segments.len().saturating_sub(1)]
        .iter()
        .position(|s| *s == "-");
    let (repo, scope) = match dash {
        Some(i) => (segments[..i].join("/"), Some(segments[i + 1..].join("/"))),
        None => (base.to_owned(), None),
    };
    let repo = repo.strip_suffix(".git").unwrap_or(&repo).to_owned();
    let scope = match scope {
        Some(s) => Some(RefName::new(s).map_err(|_| RouteError::BadName)?),
        None => None,
    };
    let repo = RepoName::new(repo).map_err(|_| RouteError::BadName)?;
    Ok(GitRoute {
        repo,
        scope,
        endpoint,
    })
}

/// A request path and query out of pieces that sit on the parser's edges.
fn built_route(input: &mut Input<'_>) -> (String, Option<String>) {
    let mut path = pick(input, &["/", "/", "/", "", "//"]).to_owned();
    let segments = 1 + input.below(4);
    for i in 0..segments {
        if i > 0 {
            path.push('/');
        }
        if input.one_in(8) {
            path.push_str(&wild(input, 8));
        } else if input.one_in(3) {
            path.push_str(pick(
                input,
                &[
                    "-", ".git", "a.git", ".x", "", "..", "%2F", "é", "a b", "x?",
                ],
            ));
        } else {
            path.push_str(pick(
                input,
                &[
                    "acme",
                    "mono",
                    "demo",
                    "a.b",
                    "x_y",
                    "a-b",
                    "A9",
                    "git-upload-pack",
                    "info",
                    "refs",
                ],
            ));
        }
    }
    path.push_str(pick(input, &["", "", ".git", ".git.git", ".GIT"]));
    if input.one_in(3) {
        path.push_str("/-/");
        if input.one_in(8) {
            path.push_str(&wild(input, 16));
        } else {
            path.push_str(pick(
                input,
                &[
                    "refs/heads/main",
                    "refs/heads/agent/7",
                    "refs/heads/x/-/y",
                    "refs/tags/v1",
                    "main",
                    "refs/heads/../x",
                    "refs/heads/a.lock",
                    "refs/heads/a b",
                    "",
                    "-",
                    "refs/heads/x.git",
                ],
            ));
        }
    }
    let end = if input.one_in(5) {
        pick(
            input,
            &[
                "/info/refs/",
                "/git-upload-pack/",
                "/HEAD",
                "/objects/info/packs",
                "/git-upload-archive",
                "",
            ],
        )
    } else {
        pick(input, &[INFO_REFS, UPLOAD_PACK, RECEIVE_PACK])
    };
    path.push_str(end);
    let query = if input.one_in(4) {
        None
    } else {
        let items: Vec<&str> = (0..1 + input.below(3))
            .map(|_| {
                pick(
                    input,
                    &[
                        "service=git-upload-pack",
                        "service=git-receive-pack",
                        "service=",
                        "x=1",
                        "service=git-upload-pack%00",
                        "SERVICE=git-upload-pack",
                        "service=git-annex",
                        "",
                    ],
                )
            })
            .collect();
        Some(items.join("&"))
    };
    (path, query)
}

/// `git_route`: [`GitRoute::parse`] gives exactly the model's answer
/// (`route_model`), and a path it routes is exactly where that route is
/// served (`render`, with or without `.git`, with no percent-decoding);
/// the other rendering routes the same, a `POST` ignores its query, and
/// the SSH transport's [`GitRoute::from_repo_path`] routes the repository
/// path alike.
pub fn git_route(data: &[u8]) {
    git_route_reached(data);
}

/// [`git_route`], returning the oracle paths the input reached.
pub fn git_route_reached(data: &[u8]) -> BTreeSet<Reached> {
    let mut input = Input::new(data);
    let (path, query) = if input.byte() % 4 == RAW {
        let raw = String::from_utf8_lossy(input.rest()).into_owned();
        match raw.split_once('?') {
            Some((p, q)) => (p.to_owned(), Some(q.to_owned())),
            None => (raw, None),
        }
    } else {
        built_route(&mut input)
    };
    let got = GitRoute::parse(&path, query.as_deref());
    assert_eq!(
        got,
        route_model(&path, query.as_deref()),
        "path {path:?}, query {query:?}"
    );
    let route = match got {
        Ok(route) => route,
        Err(e) => {
            return BTreeSet::from([match e {
                RouteError::NotGit => Reached::RouteNotGit,
                RouteError::BadName => Reached::RouteBadName,
                RouteError::DumbHttp => Reached::RouteDumbHttp,
            }]);
        }
    };
    let dot_git = if path == render(&route, false) {
        false
    } else {
        assert_eq!(
            path,
            render(&route, true),
            "{route:?} routed from elsewhere"
        );
        true
    };
    let other = render(&route, !dot_git);
    assert_eq!(
        GitRoute::parse(&other, service_query(route.endpoint)).as_ref(),
        Ok(&route),
        "{other:?} routes elsewhere"
    );
    if route.endpoint.is_post() {
        assert_eq!(GitRoute::parse(&path, None).as_ref(), Ok(&route));
    }
    let repo_path = &path[1..path.len() - suffix(route.endpoint).len()];
    assert_eq!(
        GitRoute::from_repo_path(repo_path, route.endpoint).as_ref(),
        Ok(&route),
        "the SSH form {repo_path:?} routes elsewhere"
    );
    BTreeSet::from([Reached::RouteOk])
}

// ---------------------------------------------------------------------
// ssh_command
// ---------------------------------------------------------------------

/// How Git quotes an argument for a POSIX shell (`sq_quote_buf`): in single
/// quotes, with `'` written as `'\''` and `!` as `'\!'`.
fn sq_quote(arg: &str) -> String {
    let mut out = String::from("'");
    for c in arg.chars() {
        match c {
            '\'' | '!' => {
                out.push_str("'\\");
                out.push(c);
                out.push('\'');
            }
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

/// How a POSIX shell would split `command` into words, for the subset of
/// shell syntax that only quotes: blanks separate words, single quotes and
/// backslashes quote, and a backslash before a newline is a line
/// continuation, removed. `None` when the command holds anything a shell
/// would act on (a metacharacter, an unquoted newline, which ends the
/// command as `;` does, an expansion, a glob, double quotes, a comment, an
/// assignment, a tilde prefix) or an unterminated quote. Written from
/// the shell's rules, not from Aspen's parser, so the two can disagree.
fn sh_words(command: &str) -> Option<Vec<String>> {
    let mut words: Vec<String> = Vec::new();
    let mut word: Option<String> = None;
    let mut chars = command.chars();
    while let Some(c) = chars.next() {
        let start = word.is_none();
        match c {
            ' ' | '\t' => words.extend(word.take()),
            '\'' => {
                let w = word.get_or_insert_with(String::new);
                loop {
                    match chars.next()? {
                        '\'' => break,
                        c => w.push(c),
                    }
                }
            }
            '\\' => match chars.next()? {
                '\n' => {}
                c => word.get_or_insert_with(String::new).push(c),
            },
            // A leading `~` is a tilde prefix; `=` in the first word makes
            // it an assignment.
            '~' | '#' if start => return None,
            '=' if words.is_empty() => return None,
            c if c.is_ascii_alphanumeric() || "._-/+@:,=~#%".contains(c) => {
                word.get_or_insert_with(String::new).push(c);
            }
            _ => return None,
        }
    }
    words.extend(word);
    Some(words)
}

/// What the SSH command parser answers.
type SshAnswer = Result<(Service, String), SshCommandError>;

/// The command forms Git and other clients send.
const SERVICES: &[(&str, Service)] = &[
    ("git-upload-pack", Service::UploadPack),
    ("git-receive-pack", Service::ReceivePack),
    ("git upload-pack", Service::UploadPack),
    ("git receive-pack", Service::ReceivePack),
];

/// `ssh_command`: a command quoted the way Git quotes it gets exactly the
/// model's answer (its path without one leading `/`, or `BadPath` for an
/// empty, `~` or `//` path, or `NotGit` for a control character or an
/// over-long command). Any accepted command, from either mode, is one a
/// POSIX shell would read as the same words with nothing expanded
/// (`sh_words`); its path is non-empty and has no leading `/` or `~` and
/// no control character; quoting it again reads back the same; and the SSH
/// and HTTP transports route it alike. `GIT_PROTOCOL` values select the
/// highest valid `version=<n>` item.
pub fn ssh_command(data: &[u8]) {
    ssh_command_reached(data);
}

/// [`ssh_command`], returning the oracle paths the input reached.
pub fn ssh_command_reached(data: &[u8]) -> BTreeSet<Reached> {
    let mut input = Input::new(data);
    let (command, want) = if input.byte() % 4 == RAW {
        (input.rest().to_vec(), None)
    } else {
        check_git_protocol(&mut input);
        built_command(&mut input)
    };
    let got = fuzzing::ssh_command(&command);
    let shown = String::from_utf8_lossy(&command);
    if let Some(want) = want {
        assert_eq!(got, want, "command {shown:?}");
    }
    let (service, path) = match got {
        Ok(ok) => ok,
        Err(SshCommandError::NotGit) => return BTreeSet::from([Reached::SshNotGit]),
        Err(SshCommandError::BadPath) => return BTreeSet::from([Reached::SshBadPath]),
    };
    let text = std::str::from_utf8(&command).expect("an accepted command is UTF-8");
    let words = sh_words(text)
        .unwrap_or_else(|| panic!("accepted {shown:?}, which a shell would not just split"));
    let (arg, head) = words.split_last().expect("an accepted command has words");
    let named = match head {
        [one] => one.clone(),
        [git, sub] if git == "git" => format!("git-{sub}"),
        _ => panic!("accepted {shown:?}, which a shell splits into {words:?}"),
    };
    assert_eq!(named, service.name(), "command {shown:?}");
    assert_eq!(
        arg.strip_prefix('/').unwrap_or(arg),
        path,
        "command {shown:?}"
    );
    assert!(
        !path.is_empty() && !path.starts_with(['/', '~']) && !path.chars().any(char::is_control),
        "accepted path {path:?}"
    );
    let again = format!("{} {}", service.name(), sq_quote(&format!("/{path}")));
    if again.len() <= MAX_SSH_COMMAND_LEN {
        assert_eq!(
            fuzzing::ssh_command(again.as_bytes()),
            Ok((service, path.clone())),
            "{again:?} reads back differently"
        );
    }
    let endpoint = match service {
        Service::UploadPack => Endpoint::UploadPack,
        Service::ReceivePack => Endpoint::ReceivePack,
    };
    assert_eq!(
        GitRoute::from_repo_path(&path, endpoint),
        GitRoute::parse(&format!("/{path}{}", suffix(endpoint)), None),
        "SSH and HTTP route {path:?} differently"
    );
    BTreeSet::from([Reached::SshOk])
}

/// A command as a client writes it: a service form, blanks, and a path
/// quoted by [`sq_quote`]. Returns the command and the model's answer
/// (none for a path left partly unquoted).
fn built_command(input: &mut Input<'_>) -> (Vec<u8>, Option<SshAnswer>) {
    let (form, service) = SERVICES[input.below(SERVICES.len())];
    // A newline between the service and the path ends the command in a
    // shell, so it is never one command.
    let blanks = pick(input, &[" ", " ", " ", "  ", "\n", " \n ", " \\\n "]);
    // `ssh://host/x` sends `/x`, `host:x` sends `x`.
    let mut arg = pick(input, &["/", "/", "", "//", "/~", "~"]).to_owned();
    if input.one_in(6) {
        arg.push_str(&wild(input, 32));
    } else {
        arg.push_str(pick(
            input,
            &[
                "acme/mono.git",
                "demo",
                "demo.git/-/refs/heads/agent/7",
                "a'b",
                "a!b",
                "a b",
                "it's!",
                "",
                "x\ny",
                "a\nb",
                "a\\\nb",
                "x\u{7}",
                "é/ü",
                "$(id)",
                "`id`",
                "a;b",
                "a|b",
                "a&b",
                "a>b",
                "a*",
                "a$HOME",
                "a#b",
                "a\\b",
                "a=b",
                "a~b",
            ],
        ));
    }
    if input.one_in(16) {
        arg.push_str(&"a".repeat(MAX_SSH_COMMAND_LEN));
    }
    let lead = if input.one_in(6) { " " } else { "" };
    let trail = if input.one_in(6) { " " } else { "" };
    // Some clients (and attackers) write the path unquoted, or quote only
    // part of it: then only the shell model judges the answer.
    if input.one_in(4) {
        let split = input.below(arg.len() + 1);
        let split = (0..=split)
            .rev()
            .find(|&i| arg.is_char_boundary(i))
            .unwrap_or(0);
        let (bare, quoted) = arg.split_at(split);
        let quoted = if quoted.is_empty() {
            String::new()
        } else {
            sq_quote(quoted)
        };
        let command = format!("{lead}{form}{blanks}{bare}{quoted}{trail}");
        return (command.into_bytes(), None);
    }
    let command = format!("{lead}{form}{blanks}{}{trail}", sq_quote(&arg));
    let path = arg.strip_prefix('/').unwrap_or(&arg);
    let want = if command.len() > MAX_SSH_COMMAND_LEN
        || arg.chars().any(char::is_control)
        || blanks.contains('\n')
    {
        Err(SshCommandError::NotGit)
    } else if path.is_empty() || path.starts_with(['/', '~']) {
        Err(SshCommandError::BadPath)
    } else {
        Ok((service, path.to_owned()))
    };
    (command.into_bytes(), Some(want))
}

/// `GIT_PROTOCOL` (an SSH environment variable Git sets) against its
/// model: colon-separated items, of which the highest `version=0`,
/// `version=1` or `version=2` wins, v0 when there is none.
fn check_git_protocol(input: &mut Input<'_>) {
    let value: String = (0..input.below(4))
        .map(|_| {
            if input.one_in(8) {
                wild(input, 12)
            } else {
                pick(
                    input,
                    &[
                        "version=2",
                        "version=1",
                        "version=0",
                        "version=3",
                        "version=02",
                        "version=",
                        "object-format=sha1",
                        "",
                    ],
                )
                .to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(":");
    let want = value
        .split(':')
        .filter_map(|item| match item {
            "version=2" => Some(2),
            "version=1" => Some(1),
            "version=0" => Some(0),
            _ => None,
        })
        .max()
        .map_or(ProtocolVersion::V0, |v| match v {
            2 => ProtocolVersion::V2,
            1 => ProtocolVersion::V1,
            _ => ProtocolVersion::V0,
        });
    assert_eq!(
        ProtocolVersion::requested(Some(&value)),
        want,
        "GIT_PROTOCOL {value:?}"
    );
}

// ---------------------------------------------------------------------
// gzip_body
// ---------------------------------------------------------------------

/// The request-body limit of upload-pack (`ServerConfig::max_request_body`'s
/// default, B2), used where the target checks that the two readers agree.
const BODY_LIMIT: usize = 16 << 20;

/// The largest body `gzip_body` builds.
const BOMB: usize = 256 << 10;

/// A gzip stream the target built, and what it knows about it.
#[derive(Debug)]
struct Built {
    /// The whole body as sent.
    stream: Vec<u8>,
    /// The body the first gzip member inflates to.
    plain: Vec<u8>,
    /// The length of the first gzip member.
    member: usize,
    /// Whether the first member was damaged (cut short, or a byte flipped
    /// where it may matter); a damaged member may still inflate.
    damaged: bool,
    /// What was done to the stream after it was written, for messages.
    change: String,
}

fn gzip(plain: &[u8], input: &mut Input<'_>) -> Vec<u8> {
    let level = flate2::Compression::new(u32::from(input.byte() % 10));
    let mut builder = flate2::GzBuilder::new().mtime(input.u32());
    if input.one_in(4) {
        builder = builder.filename(b"request".to_vec());
    }
    if input.one_in(4) {
        builder = builder.comment(b"from a fuzzer".to_vec());
    }
    if input.one_in(4) {
        builder = builder.extra(input.bytes(16).to_vec());
    }
    let mut w = builder.write(Vec::new(), level);
    w.write_all(plain).expect("in-memory write");
    w.finish().expect("in-memory write")
}

fn built_gzip(input: &mut Input<'_>) -> Built {
    let pattern = input.bytes(64).to_vec();
    // Mostly a short body; sometimes one that inflates far past what it
    // costs to send (a zip bomb), up to 256 KiB. The limits each check
    // picks sit around the body's own sizes, so a bigger bomb would only
    // slow the target down (three readers inflate every body).
    let repeat = if input.one_in(8) {
        1 + input.below(BOMB)
    } else {
        1 + input.below(64)
    };
    let plain: Vec<u8> = if pattern.is_empty() {
        vec![0; input.below(repeat + 1)]
    } else {
        pattern
            .iter()
            .copied()
            .cycle()
            .take((pattern.len() * repeat).min(BOMB))
            .collect()
    };
    let mut stream = gzip(&plain, input);
    let member = stream.len();
    let mut damaged = false;
    let change = match input.below(8) {
        // Cut short: the member's trailer (or more) is missing.
        0 => {
            let at = input.below(member);
            stream.truncate(at);
            damaged = true;
            format!("cut at {at}")
        }
        // A flipped bit. The header's MTIME, XFL and OS bytes (4 to 9)
        // are never checked, so a flip there leaves the member valid.
        1 => {
            let at = input.below(member);
            let bit = input.byte() % 8;
            stream[at] ^= 1 << bit;
            damaged = !(4..10).contains(&at);
            format!("bit {bit} of byte {at} flipped")
        }
        // Bytes after the member: only the first member is read.
        2 => {
            let tail = input.bytes(32);
            stream.extend_from_slice(tail);
            format!("{} bytes appended", tail.len())
        }
        3 => {
            let second = gzip(b"a second member", input);
            stream.extend_from_slice(&second);
            "a second member appended".to_owned()
        }
        _ => "none".to_owned(),
    };
    Built {
        stream,
        plain,
        member,
        damaged,
        change,
    }
}

/// Splits `data` into chunks of random sizes, as a connection delivers a
/// body.
fn chunks(data: &[u8], mix: &mut Mix) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut rest = data;
    while !rest.is_empty() {
        let n = match mix.below(3) {
            0 => 1 + mix.below(8),
            1 => 1 + mix.below(4096),
            _ => rest.len(),
        }
        .min(rest.len());
        out.push(rest[..n].to_vec());
        rest = &rest[n..];
    }
    out
}

/// A reader that hands out its bytes in chunks of random sizes.
struct Trickle<'a> {
    data: &'a [u8],
    mix: Mix,
}

impl Read for Trickle<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = match self.mix.below(3) {
            0 => 1,
            1 => 1 + self.mix.below(64),
            _ => buf.len(),
        }
        .min(buf.len())
        .min(self.data.len());
        buf[..n].copy_from_slice(&self.data[..n]);
        self.data = &self.data[n..];
        Ok(n)
    }
}

/// `Content-Encoding` values: the ones Aspen reads and its near misses.
const ENCODINGS: &[Option<&[u8]>] = &[
    Some(b"gzip"),
    Some(b"gzip"),
    Some(b"x-gzip"),
    Some(b" GZIP "),
    Some(b"identity"),
    Some(b""),
    None,
    Some(b"br"),
    Some(b"gzip, identity"),
    Some(b"\xffgzip"),
];

/// The model of `Content-Encoding`: the header as text (bytes outside
/// visible ASCII and tab make it unreadable, named `?`), trimmed and
/// lowercased; identity, gzip, or the name of an encoding Aspen refuses.
fn encoding_model(value: Option<&[u8]>) -> Result<bool, String> {
    let Some(value) = value else {
        return Ok(false);
    };
    let text = if value
        .iter()
        .all(|&b| b == b'\t' || (0x20..0x7f).contains(&b))
    {
        String::from_utf8(value.to_vec()).expect("ASCII")
    } else {
        "?".to_owned()
    };
    match text.trim().to_ascii_lowercase().as_str() {
        "" | "identity" => Ok(false),
        "gzip" | "x-gzip" => Ok(true),
        other => Err(other.to_owned()),
    }
}

fn near(n: usize, input: &mut Input<'_>) -> usize {
    match input.below(3) {
        0 => n.saturating_sub(1),
        1 => n,
        _ => n + 1,
    }
}

/// `gzip_body`: upload-pack's body reader (bounded as sent and as inflated
/// to one limit) and receive-pack's gzip push reader (charged to a push
/// meter as the larger of sent and inflated bytes, and stopped past a
/// ceiling of sent bytes) on a body the target built: each gives exactly
/// the model's answer where the model can tell, never inflates a damaged
/// member to anything but its body, and the meter is charged at least
/// every byte sent and every byte inflated. On every input, raw ones too,
/// the two readers agree: one inflates a body exactly when the other
/// inflates the same body.
pub fn gzip_body(data: &[u8]) {
    gzip_body_reached(data);
}

/// [`gzip_body`], returning the oracle paths the input reached.
pub fn gzip_body_reached(data: &[u8]) -> BTreeSet<Reached> {
    let mut reached = BTreeSet::new();
    let mut input = Input::new(data);
    let raw = input.byte() % 4 == RAW;
    let mut mix = Mix::new(input.u64());
    if raw {
        let stream = input.rest();
        check_readers_agree(stream, &mut mix, &mut reached);
        return reached;
    }
    let built = built_gzip(&mut input);
    check_read_body(&built, &mut input, &mut mix, &mut reached);
    check_gzip_push(&built, &mut input, &mut mix, &mut reached);
    check_readers_agree(&built.stream, &mut mix, &mut reached);
    reached
}

fn check_read_body(
    built: &Built,
    input: &mut Input<'_>,
    mix: &mut Mix,
    reached: &mut BTreeSet<Reached>,
) {
    let encoding = ENCODINGS[input.below(ENCODINGS.len())];
    let limit = match input.below(4) {
        0 => near(built.plain.len(), input),
        1 => near(built.stream.len(), input),
        2 => BODY_LIMIT,
        _ => input.below(4096),
    };
    let got = fuzzing::read_body(encoding, chunks(&built.stream, mix), limit);
    let context = || {
        format!(
            "encoding {encoding:?}, limit {limit}, {} bytes sent, {} inflated, member {}, \
             change {}: {got:?}",
            built.stream.len(),
            built.plain.len(),
            built.member,
            built.change
        )
    };
    let fits = built.plain.len() <= limit;
    if built.stream.len() > limit {
        assert_eq!(got, Err(ReadBodyError::TooLarge), "{}", context());
    } else {
        match encoding_model(encoding) {
            Err(name) => assert_eq!(got, Err(ReadBodyError::Encoding(name)), "{}", context()),
            Ok(false) => assert_eq!(got.as_ref(), Ok(&built.stream), "{}", context()),
            Ok(true) if !built.damaged => {
                let want = if fits {
                    Ok(&built.plain)
                } else {
                    Err(&ReadBodyError::TooLarge)
                };
                assert_eq!(got.as_ref(), want, "{}", context());
            }
            // A damaged member may inflate to anything, of any size, before
            // its CRC is checked; only its body may come out.
            Ok(true) => match &got {
                Ok(body) => assert!(*body == built.plain && fits, "{}", context()),
                Err(ReadBodyError::Read(_) | ReadBodyError::TooLarge) => {}
                Err(e) => panic!("{e:?}: {}", context()),
            },
        }
    }
    reached.insert(match got {
        Ok(_) => Reached::GzipOk,
        Err(ReadBodyError::TooLarge) => Reached::GzipTooLarge,
        Err(ReadBodyError::Encoding(_)) => Reached::GzipEncoding,
        Err(ReadBodyError::Read(_)) => Reached::GzipCorrupt,
    });
}

fn check_gzip_push(
    built: &Built,
    input: &mut Input<'_>,
    mix: &mut Mix,
    reached: &mut BTreeSet<Reached>,
) {
    let (p, m, l) = (
        built.plain.len() as u64,
        built.member as u64,
        built.stream.len() as u64,
    );
    let meter = match input.below(4) {
        0 => None,
        1 => Some(None),
        _ => Some(Some(match input.below(3) {
            0 => near(built.plain.len(), input) as u64,
            1 => near(built.member, input) as u64,
            _ => near(built.stream.len(), input) as u64,
        })),
    };
    let max_sent = match input.below(3) {
        0 => near(built.member, input) as u64,
        1 => near(built.stream.len(), input) as u64,
        _ => u64::MAX,
    };
    let got = fuzzing::gzip_push(
        Trickle {
            data: &built.stream,
            mix: mix.clone(),
        },
        meter,
        max_sent,
    );
    mix.next_u64();
    let context = || {
        format!(
            "meter {meter:?}, max sent {max_sent}, {l} bytes sent, {p} inflated, member {m}, \
             change {}: {got:?}",
            built.change
        )
    };
    let cap = meter.flatten().unwrap_or(u64::MAX);
    match &got.inflated {
        Ok(body) => {
            assert!(*body == built.plain, "{}", context());
            assert!(p <= cap && m <= cap && m <= max_sent, "{}", context());
            if meter.is_some() {
                // At least every byte of the member was sent, and at most
                // every byte there is.
                assert!(
                    got.charged >= m.max(p) && got.charged <= l.max(p),
                    "{}",
                    context()
                );
            } else {
                assert_eq!(got.charged, 0, "{}", context());
            }
        }
        Err(GzipPushError::OverCap) => {
            assert!(meter.flatten().is_some(), "{}", context());
            assert!(got.charged <= cap, "{}", context());
        }
        Err(GzipPushError::SentTooMuch) => assert!(l > max_sent, "{}", context()),
        Err(GzipPushError::Corrupt(_)) => {}
    }
    if !built.damaged {
        // Whatever follows the member, and whichever limit stops the read,
        // an undamaged member is never reported corrupt.
        assert!(
            !matches!(got.inflated, Err(GzipPushError::Corrupt(_))),
            "{}",
            context()
        );
        let over_cap = p > cap || m > cap;
        let must_fail = over_cap || m > max_sent;
        let must_pass = !must_fail && l <= max_sent && l <= cap;
        if must_pass {
            assert!(got.inflated.is_ok(), "{}", context());
        }
        if must_fail {
            let err = got.inflated.as_ref().expect_err("over a limit");
            if over_cap && l <= max_sent {
                assert_eq!(*err, GzipPushError::OverCap, "{}", context());
            }
            if !over_cap && l <= cap {
                assert_eq!(*err, GzipPushError::SentTooMuch, "{}", context());
            }
        }
    }
    reached.insert(match got.inflated {
        Ok(_) => Reached::GzipOk,
        Err(GzipPushError::OverCap) => Reached::GzipOverCap,
        Err(GzipPushError::SentTooMuch) => Reached::GzipSentTooMuch,
        Err(GzipPushError::Corrupt(_)) => Reached::GzipCorrupt,
    });
}

/// Upload-pack's reader and receive-pack's agree on `stream`, at the same
/// 16 MiB bound: one inflates it to a body exactly when the other inflates
/// it to the same body.
fn check_readers_agree(stream: &[u8], mix: &mut Mix, reached: &mut BTreeSet<Reached>) {
    let fetch = fuzzing::read_body(Some(b"gzip"), chunks(stream, mix), BODY_LIMIT);
    let push = fuzzing::gzip_push(
        Trickle {
            data: stream,
            mix: mix.clone(),
        },
        Some(Some(BODY_LIMIT as u64)),
        u64::MAX,
    );
    match (&fetch, &push.inflated) {
        (Ok(a), Ok(b)) => assert!(a == b, "the readers inflate {stream:?} differently"),
        (Ok(_), Err(e)) => panic!("upload-pack inflates {stream:?}, receive-pack refuses: {e:?}"),
        (Err(e), Ok(_)) => panic!("receive-pack inflates {stream:?}, upload-pack refuses: {e:?}"),
        (Err(_), Err(_)) => {}
    }
    reached.insert(match fetch {
        Ok(_) => Reached::GzipOk,
        Err(ReadBodyError::TooLarge) => Reached::GzipTooLarge,
        Err(_) => Reached::GzipCorrupt,
    });
}
