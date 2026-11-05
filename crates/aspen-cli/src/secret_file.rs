//! Secret files: `ASPEN_SSH_HOST_KEY_FILE`, `ASPEN_MIRROR_<NAME>_TOKEN_FILE`
//! and `ASPEN_MIRROR_<NAME>_GITHUB_PRIVATE_KEY_FILE` (issue #125).
//!
//! Each is opened once (following symbolic links: Kubernetes mounts Secrets
//! through them), and the type and mode of the open file (`fstat`) decide
//! whether it is read, from the same handle, so the file checked is the
//! file read:
//!
//! - anything but a regular file (a directory, a FIFO, a device) is
//!   refused. The file is opened without blocking, so a FIFO with no writer
//!   cannot hold up startup, and a device is never read;
//! - any permission for other users (`chmod o=...`) refuses the file, and
//!   the command does not start: any account on the machine could read the
//!   secret, or replace it;
//! - any permission for the file's group is accepted silently when that
//!   group is one of Aspen's own (its effective group or a supplementary
//!   one: a Kubernetes Secret mounted with the pod's `fsGroup` is always
//!   group-readable, and the Helm chart runs in that group), and logs a
//!   warning otherwise.
//!
//! The rule is [`exposure`], a pure function of the mode and the group IDs,
//! so it is tested without touching the file system.

use std::path::Path;

/// Who besides the owner may access a secret file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Exposure {
    /// The owner only.
    Owner,
    /// The owner and the file's group, which is one of Aspen's groups.
    OwnGroup,
    /// The owner and a group Aspen does not run in: logged as a warning.
    OtherGroup,
    /// Every user of the machine: refused.
    Everyone,
}

/// Classifies a file's permission bits (`mode`, as `st_mode`), given the
/// file's group and the process's groups (effective and supplementary).
pub(crate) fn exposure(mode: u32, file_gid: u32, own_gids: &[u32]) -> Exposure {
    if mode & 0o007 != 0 {
        Exposure::Everyone
    } else if mode & 0o070 == 0 {
        Exposure::Owner
    } else if own_gids.contains(&file_gid) {
        Exposure::OwnGroup
    } else {
        Exposure::OtherGroup
    }
}

/// The process's effective group and its supplementary groups.
#[cfg(unix)]
fn own_gids() -> Vec<u32> {
    let mut gids = vec![rustix::process::getegid().as_raw()];
    // Without the list, a foreign-looking group only costs a warning.
    if let Ok(extra) = rustix::process::getgroups() {
        gids.extend(extra.into_iter().map(|g| g.as_raw()));
    }
    gids
}

/// Opens a secret file, checks the type and mode of what was opened, and
/// reads it from the same handle. The error (not a regular file, or a
/// refused mode) names the problem and the fix, never the content; callers
/// add the variable and path. A file a foreign group may access is read,
/// with a warning naming the path.
pub(crate) fn read(path: &Path) -> std::io::Result<String> {
    use std::io::Read;

    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // Opening a FIFO waits for a writer unless it is non-blocking; it is
        // then refused below. Reads of a regular file ignore the flag.
        options.custom_flags(rustix::fs::OFlags::NONBLOCK.bits().cast_signed());
    }
    let mut file = options.open(path)?;
    let meta = file.metadata()?;
    if !meta.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "not a regular file; point it at the file holding the secret",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let mode = meta.mode() & 0o777;
        match exposure(mode, meta.gid(), &own_gids()) {
            Exposure::Owner | Exposure::OwnGroup => {}
            Exposure::OtherGroup => tracing::warn!(
                "secret file {} is accessible to group {} (mode {mode:04o}), which aspen \
                 does not run in; restrict it with chmod 600",
                path.display(),
                meta.gid(),
            ),
            Exposure::Everyone => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!(
                        "accessible to every user (mode {mode:04o}); restrict it with \
                         chmod 600 (or 640 for aspen's own group)"
                    ),
                ));
            }
        }
    }
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn other_users_may_never_access_a_secret_file() {
        for mode in [0o604, 0o602, 0o601, 0o644, 0o666, 0o777, 0o407] {
            assert_eq!(exposure(mode, 10, &[10]), Exposure::Everyone, "{mode:o}");
        }
    }

    #[test]
    fn a_group_readable_file_is_quiet_only_for_aspens_own_groups() {
        for mode in [0o640, 0o440, 0o660, 0o620] {
            assert_eq!(
                exposure(mode, 10001, &[10001]),
                Exposure::OwnGroup,
                "{mode:o}"
            );
            // A supplementary group (the pod's fsGroup) counts as Aspen's own.
            assert_eq!(
                exposure(mode, 2000, &[10001, 2000]),
                Exposure::OwnGroup,
                "{mode:o}"
            );
            assert_eq!(
                exposure(mode, 100, &[10001, 2000]),
                Exposure::OtherGroup,
                "{mode:o}"
            );
        }
    }

    #[test]
    fn an_owner_only_file_is_accepted_whatever_its_group() {
        for mode in [0o600, 0o400, 0o700, 0o000] {
            assert_eq!(exposure(mode, 100, &[10001]), Exposure::Owner, "{mode:o}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_world_readable_file_is_refused_before_it_is_read() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "s3cret-value\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let e = read(&path).unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied);
        let text = e.to_string();
        assert!(
            text.contains("mode 0644") && text.contains("chmod 600"),
            "{text}"
        );
        assert!(!text.contains("s3cret"), "secret echoed: {text}");

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(read(&path).unwrap(), "s3cret-value\n");
    }

    #[cfg(unix)]
    #[test]
    fn only_a_regular_file_is_read() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("fifo");
        let made = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(made.success());
        std::fs::set_permissions(&fifo, std::fs::Permissions::from_mode(0o600)).unwrap();
        let sub = dir.path().join("dir");
        std::fs::create_dir(&sub).unwrap();
        std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o700)).unwrap();

        // A FIFO with no writer would block a plain open forever.
        let (tx, rx) = std::sync::mpsc::channel();
        let paths = [fifo, sub, "/dev/null".into()];
        std::thread::spawn(move || {
            for p in paths {
                tx.send((p.clone(), read(&p))).unwrap();
            }
        });
        for _ in 0..3 {
            let (p, got) = rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("reading a non-regular file blocked");
            let e = got.expect_err("only regular files are read");
            assert_eq!(
                e.kind(),
                std::io::ErrorKind::InvalidInput,
                "{}",
                p.display()
            );
            assert!(e.to_string().contains("not a regular file"), "{e}");
        }
    }
}
