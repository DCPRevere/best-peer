//! The files `approve` and `revoke` read from the data directory, read so that the tool, when it runs as another user
//! than the directory's owner (root on a host where the peer has its own user), reads nothing that owner could not
//! have put there: on Unix each file is then opened without following a link and without waiting on a pipe, and must
//! be a plain file of that owner's, 64 KiB at most. Run as the owner, as on a machine with one user, a file
//! is read as it is, a linked data directory included. Windows has no such owner to compare with, and reads as it is.

use std::path::Path;

/// The most a file of the data directory read by the authority may hold: the token and the record are far smaller.
#[cfg(unix)]
pub const BOUND: u64 = 64 * 1024;

/// The text of `name` in `dir`, read as the module says.
pub fn read(dir: &Path, name: &str) -> std::io::Result<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let owner = std::fs::metadata(dir)?.uid();
        if rustix::process::geteuid().as_raw() != owner {
            return read_as_another(&dir.join(name), owner);
        }
    }
    std::fs::read_to_string(dir.join(name))
}

/// The text of `path`, a plain file of `owner`'s of [`BOUND`] bytes at most, opened without following a link or
/// waiting on a pipe; anything else is refused, naming the file and why.
#[cfg(unix)]
fn read_as_another(path: &Path, owner: u32) -> std::io::Result<String> {
    use rustix::fs::{Mode, OFlags};
    use std::io::Read;
    use std::os::unix::fs::MetadataExt;
    let refused = |why: &str| {
        std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "{} {why}; this command runs as another user than the data directory's owner, so it reads only a plain file of that owner's",
                path.display()
            ),
        )
    };
    let fd = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|e| match std::fs::symlink_metadata(path) {
        Ok(m) if m.file_type().is_symlink() => refused("is a link"),
        _ => std::io::Error::from(e),
    })?;
    let file = std::fs::File::from(fd);
    let meta = file.metadata()?;
    if !meta.file_type().is_file() {
        return Err(refused("is not a plain file"));
    }
    if meta.uid() != owner {
        return Err(refused("belongs to another user"));
    }
    if meta.len() > BOUND {
        return Err(refused(&format!("holds more than {BOUND} bytes")));
    }
    let mut text = String::new();
    file.take(BOUND + 1).read_to_string(&mut text)?;
    if text.len() as u64 > BOUND {
        return Err(refused(&format!("holds more than {BOUND} bytes")));
    }
    Ok(text)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    fn me(dir: &Path) -> u32 {
        std::fs::metadata(dir).unwrap().uid()
    }

    #[test]
    fn run_as_the_owner_a_file_is_read_as_it_is_a_link_included() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("elsewhere"), "token").unwrap();
        std::os::unix::fs::symlink(dir.path().join("elsewhere"), dir.path().join("admin-token")).unwrap();
        assert_eq!(read(dir.path(), "admin-token").unwrap(), "token");
    }

    #[test]
    fn run_as_another_a_plain_file_of_the_owner_is_read() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("admin-token"), "token").unwrap();
        assert_eq!(
            read_as_another(&dir.path().join("admin-token"), me(dir.path())).unwrap(),
            "token"
        );
    }

    #[test]
    fn run_as_another_a_link_is_refused_and_not_followed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("secret"), "not for the door").unwrap();
        std::os::unix::fs::symlink(dir.path().join("secret"), dir.path().join("admin-token")).unwrap();
        let e = read_as_another(&dir.path().join("admin-token"), me(dir.path())).unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(e.to_string().contains("is a link"), "{e}");
    }

    #[test]
    fn run_as_another_a_file_of_another_user_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("admin-token"), "token").unwrap();
        let e = read_as_another(&dir.path().join("admin-token"), me(dir.path()) + 1).unwrap_err();
        assert!(e.to_string().contains("belongs to another user"), "{e}");
    }

    #[test]
    fn run_as_another_a_pipe_is_refused_without_waiting() {
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("admin-token");
        rustix::fs::mknodat(
            rustix::fs::CWD,
            &fifo,
            rustix::fs::FileType::Fifo,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
            0,
        )
        .unwrap();
        let e = read_as_another(&fifo, me(dir.path())).unwrap_err();
        assert!(e.to_string().contains("is not a plain file"), "{e}");
    }

    #[test]
    fn run_as_another_a_file_past_the_bound_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("door"), vec![b'x'; BOUND as usize + 1]).unwrap();
        let e = read_as_another(&dir.path().join("door"), me(dir.path())).unwrap_err();
        assert!(e.to_string().contains("more than"), "{e}");
    }
}
