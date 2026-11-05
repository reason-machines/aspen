//! The command an SSH client asks to run, as Git sends it:
//!
//! ```text
//! git-upload-pack '/acme/mono.git'                 ssh://host/acme/mono.git
//! git-receive-pack 'acme/mono.git'                 host:acme/mono.git (scp-like)
//! git-receive-pack '/acme/mono.git/-/refs/heads/x' a branch-scoped URL (S2)
//! git upload-pack '/acme/mono.git'                 the two-word form
//! ```
//!
//! Git quotes the path for a POSIX shell (`sq_quote_buf`): single quotes,
//! with `'` written as `'\''` and `!` as `'\!'`. Aspen runs no shell, so
//! it reads that quoting itself and accepts nothing else: a word may mix
//! single-quoted text, backslash-escaped `'` or `!`, and unquoted
//! characters from a small safe set. Any other shell syntax (`;`, `|`,
//! `$`, backticks, double quotes, redirections, globs) is refused rather
//! than interpreted, and so is anything but exactly a service and one
//! path.

use aspen_proto::service::Service;

/// Longest command accepted, in bytes. A path is a repository name of at
/// most a few hundred bytes plus a ref of at most 1,024.
pub(crate) const MAX_COMMAND_LEN: usize = 4096;

/// A Git command: the service and the repository path it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GitCommand {
    /// `git-upload-pack` or `git-receive-pack`.
    pub(crate) service: Service,
    /// The path without its leading `/`: `<repo>[.git][/-/<ref>]`.
    pub(crate) path: String,
}

/// Why a command is not served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CommandError {
    /// Not a Git service Aspen runs (a shell command, `git-upload-archive`).
    NotGit,
    /// A Git service with an argument Aspen cannot read.
    BadPath,
}

/// Parses a command line (the payload of an SSH `exec` request).
pub(crate) fn parse(command: &[u8]) -> Result<GitCommand, CommandError> {
    if command.len() > MAX_COMMAND_LEN {
        return Err(CommandError::NotGit);
    }
    let command = std::str::from_utf8(command).map_err(|_| CommandError::NotGit)?;
    let words = words(command).ok_or(CommandError::NotGit)?;
    let (service, args) = match words.as_slice() {
        [first, rest @ ..] if first == "git" => match rest {
            [sub, args @ ..] => (service(&format!("git-{sub}"))?, args),
            [] => return Err(CommandError::NotGit),
        },
        [first, args @ ..] => (service(first)?, args),
        [] => return Err(CommandError::NotGit),
    };
    let [path] = args else {
        return Err(CommandError::BadPath);
    };
    // `ssh://host/x` sends `/x`; `host:x` sends `x`. Both name the same
    // repository. Home-relative paths (`~user/x`) name nothing here.
    let path = path.strip_prefix('/').unwrap_or(path);
    if path.is_empty() || path.starts_with('~') || path.starts_with('/') {
        return Err(CommandError::BadPath);
    }
    Ok(GitCommand {
        service,
        path: path.to_owned(),
    })
}

fn service(name: &str) -> Result<Service, CommandError> {
    Service::parse(name).ok_or(CommandError::NotGit)
}

/// Characters a word may hold outside quotes.
fn safe(c: char) -> bool {
    c.is_ascii_alphanumeric()
        || matches!(c, '.' | '_' | '-' | '/' | '~' | '+' | '@' | ':' | ',' | '=')
}

/// Splits a command into words with the quoting described in the module
/// docs; `None` for anything else.
fn words(command: &str) -> Option<Vec<String>> {
    let mut words = Vec::new();
    let mut word: Option<String> = None;
    let mut chars = command.chars();
    while let Some(c) = chars.next() {
        match c {
            ' ' => {
                if let Some(w) = word.take() {
                    words.push(w);
                }
            }
            '\'' => {
                let w = word.get_or_insert_with(String::new);
                loop {
                    match chars.next()? {
                        '\'' => break,
                        c if c.is_control() => return None,
                        c => w.push(c),
                    }
                }
            }
            '\\' => match chars.next()? {
                c @ ('\'' | '!') => word.get_or_insert_with(String::new).push(c),
                _ => return None,
            },
            c if safe(c) => word.get_or_insert_with(String::new).push(c),
            _ => return None,
        }
    }
    words.extend(word);
    Some(words)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(command: &str) -> GitCommand {
        parse(command.as_bytes()).unwrap_or_else(|e| panic!("{command}: {e:?}"))
    }

    #[test]
    fn reads_what_git_sends() {
        let c = ok("git-upload-pack '/acme/mono.git'");
        assert_eq!(c.service, Service::UploadPack);
        assert_eq!(c.path, "acme/mono.git");
        let c = ok("git-receive-pack 'acme/mono.git'");
        assert_eq!(c.service, Service::ReceivePack);
        assert_eq!(c.path, "acme/mono.git");
        assert_eq!(
            ok("git upload-pack '/acme/mono.git/-/refs/heads/agent/7'").path,
            "acme/mono.git/-/refs/heads/agent/7"
        );
        assert_eq!(ok("git-upload-pack /demo.git").path, "demo.git");
        assert_eq!(ok("git-upload-pack  'demo'").path, "demo");
    }

    /// Git's `sq_quote_buf` writes `'` as `'\''` and `!` as `'\!'`; those
    /// read back as the original characters (the route then refuses them
    /// as repository names).
    #[test]
    fn reads_gits_shell_quoting() {
        assert_eq!(ok(r"git-upload-pack '/a'\''b'").path, "a'b");
        assert_eq!(ok(r"git-upload-pack '/a'\!'b'").path, "a!b");
        assert_eq!(ok("git-upload-pack '/a'b'c'").path, "abc");
    }

    /// Invariant: nothing but a Git service and exactly one path is
    /// accepted, and no shell syntax is interpreted: a command that would
    /// do anything else in a shell is refused.
    #[test]
    fn refuses_everything_else() {
        use CommandError::*;
        let cases = [
            ("", NotGit),
            ("ls", NotGit),
            ("git", NotGit),
            ("git status", NotGit),
            ("git-upload-archive '/demo.git'", NotGit),
            ("scp -t /tmp", NotGit),
            ("git-upload-pack '/demo.git'; rm -rf /", NotGit),
            ("git-upload-pack '/demo.git' && id", NotGit),
            ("git-upload-pack `id`", NotGit),
            ("git-upload-pack $(id)", NotGit),
            ("git-upload-pack \"/demo.git\"", NotGit),
            ("git-upload-pack '/demo.git' > /tmp/x", NotGit),
            ("git-upload-pack /demo*", NotGit),
            ("git-upload-pack '/demo.git", NotGit),
            ("git-upload-pack '/demo\n.git'", NotGit),
            ("git-upload-pack\t'/demo.git'", NotGit),
            (r"git-upload-pack '/a'\x'b'", NotGit),
            ("git-upload-pack", BadPath),
            ("git-upload-pack '/a' '/b'", BadPath),
            ("git-upload-pack ''", BadPath),
            ("git-upload-pack '/'", BadPath),
            ("git-upload-pack '~alice/demo.git'", BadPath),
            ("git-upload-pack '//demo.git'", BadPath),
        ];
        for (command, want) in cases {
            assert_eq!(parse(command.as_bytes()), Err(want), "{command:?}");
        }
        assert_eq!(parse(&[0xff, 0x20]), Err(NotGit));
        let long = format!("git-upload-pack '/{}'", "a".repeat(MAX_COMMAND_LEN));
        assert_eq!(parse(long.as_bytes()), Err(NotGit));
    }
}
