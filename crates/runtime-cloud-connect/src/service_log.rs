/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! The bounded log files a supervised `spiced` writes, and the reader that
//! prints them.
//!
//! A supervisor that owns no log store — launchd writes whatever a job prints
//! to the files its definition names, and never bounds them — leaves the
//! runtime to bound its own output or fill the disk. So a managed instance is
//! started with `--service-log-dir <path>` and writes there under one fixed
//! policy: [`RETAINED_FILES`] files of about [`MAX_FILE_BYTES`] each, plain
//! text, no compression, oldest discarded first.
//!
//! "About", not "at most": a record is never split across files, so one that is
//! larger than [`MAX_FILE_BYTES`] on its own overflows the file it lands in —
//! it has nowhere smaller to go. A line divided between two files is a line no
//! search finds in either, which is the worse failure of the two, so the exact
//! bound is what gives way. Every other record starts a new file rather than
//! overflowing the current one, so the ceiling is `max(MAX_FILE_BYTES, largest
//! record)` per file.
//!
//! The policy is fixed rather than configurable because both halves of it live
//! in different binaries — `spiced` writes the files and `spice` reads them —
//! and a setting only one half knew about would silently mean the reader looked
//! for files the writer never made. Both halves are here, against the same
//! constants, for that reason.
//!
//! ## Following across rotation
//!
//! Rotation renames the live file out from under any reader holding it open, so
//! a follower that only ever reads its open descriptor goes quiet the moment
//! the file it is watching becomes `spiced.log.1`. [`ServiceLogReader::follow`]
//! therefore compares the identity of the file on the live *path* with the one
//! it holds open on every poll, drains what remains of the old file, and
//! reopens. A file that shrank instead of being renamed was truncated in place,
//! and is reopened from the beginning for the same reason.

use std::fs::{File, OpenOptions};
use std::io::{self, Read as _, Seek as _, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Files kept for one service: the live file plus its rotations.
pub const RETAINED_FILES: usize = 5;

/// How large one file grows before it is rotated.
///
/// A record is never split across files, so a file exceeds this only when a
/// single record does: the ceiling is `max(MAX_FILE_BYTES, largest record)`,
/// and the set as a whole is bounded by [`RETAINED_FILES`] times that.
pub const MAX_FILE_BYTES: u64 = 10 * 1024 * 1024;

/// The file a supervised runtime is writing right now.
pub const LIVE_FILE_NAME: &str = "spiced.log";

/// How often a follower looks for new output.
const FOLLOW_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// How much of a file is read at a time while collecting its last lines.
const TAIL_CHUNK_BYTES: u64 = 64 * 1024;

/// Bytes read from one file in a single follow poll, so a writer that is faster
/// than the reader cannot make one poll unbounded.
const FOLLOW_READ_LIMIT: usize = 1024 * 1024;

/// The file the runtime is writing to in `dir`.
#[must_use]
pub fn live_path(dir: &Path) -> PathBuf {
    dir.join(LIVE_FILE_NAME)
}

/// The `index`-th rotated file, where `1` is the most recently rotated.
#[must_use]
pub fn rotated_path(dir: &Path, index: usize) -> PathBuf {
    dir.join(format!("{LIVE_FILE_NAME}.{index}"))
}

/// Every file of the policy in the order the runtime wrote them: oldest
/// rotation first, the live file last.
#[must_use]
pub fn rotation_set(dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = (1..RETAINED_FILES)
        .rev()
        .map(|i| rotated_path(dir, i))
        .collect();
    paths.push(live_path(dir));
    paths
}

/// Identity of an open file, so a reader can tell "the same file, longer" from
/// "a different file under the same name".
#[cfg(unix)]
fn file_identity(metadata: &std::fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt as _;
    (metadata.dev(), metadata.ino())
}

// No `#[cfg(not(unix))]` counterpart: a fabricated identity would make every
// file compare equal, so a follower would seek into the replacement file with
// the rotated file's offset and silently drop output. The follower is therefore
// compiled only where a real identity exists — which is also the only place a
// supervisor this crate serves runs.

/// The bounded, rotating sink a supervised runtime writes its console output
/// to.
///
/// Rotation is by rename, not by copy-and-truncate: a reader holding the file
/// open keeps reading the bytes it was already given rather than seeing them
/// vanish mid-line, and the live path is replaced by a fresh, empty file that
/// the reader can detect by identity.
#[derive(Debug)]
pub struct RotatingLog {
    dir: PathBuf,
    file: File,
    /// Bytes in the open file. Tracked rather than restatted per write: this is
    /// on the path of every log line the runtime emits.
    written: u64,
}

impl RotatingLog {
    /// Open (or create) the live file in `dir`, creating `dir` if it is absent.
    ///
    /// Appends rather than truncates: a restarted service continues the file it
    /// was writing, so the history an operator is reading does not disappear
    /// every time the supervisor brings the runtime back.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory or the file cannot be created.
    pub fn open(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = live_path(dir);
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let written = file.metadata()?.len();
        Ok(Self {
            dir: dir.to_path_buf(),
            file,
            written,
        })
    }

    /// Shift every retained file down one place and start a new live file.
    ///
    /// The oldest is removed first so the set never briefly holds more than
    /// [`RETAINED_FILES`] files, which is the bound the policy promises.
    fn rotate(&mut self) -> io::Result<()> {
        let _ = std::fs::remove_file(rotated_path(&self.dir, RETAINED_FILES - 1));
        for index in (1..RETAINED_FILES - 1).rev() {
            let from = rotated_path(&self.dir, index);
            let to = rotated_path(&self.dir, index + 1);
            match std::fs::rename(&from, &to) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        let live = live_path(&self.dir);
        match std::fs::rename(&live, rotated_path(&self.dir, 1)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        self.file = OpenOptions::new().append(true).create(true).open(&live)?;
        self.written = 0;
        Ok(())
    }
}

impl Write for RotatingLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Rotation happens between records rather than inside one: a line split
        // across two files is a line no search finds in either. So a record
        // that would overflow the live file starts a new one instead of being
        // appended to it, and the only way a file exceeds the bound is a single
        // record that exceeds it alone — which has nowhere smaller to go. That
        // makes the per-file ceiling `max(MAX_FILE_BYTES, one record)` rather
        // than `MAX_FILE_BYTES + one record`.
        let record = buf.len() as u64;
        if self.written > 0 && self.written.saturating_add(record) > MAX_FILE_BYTES {
            self.rotate()?;
        }
        // `write_all`, not `write`: a short write would return here and the
        // caller's next call could rotate between two halves of one record,
        // which is the split this design exists to avoid. One call in, one
        // whole record on disk.
        self.file.write_all(buf)?;
        self.written += record;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// Where a follower resumes from: the file it should be reading, and how far
/// into it the caller has already seen.
#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FollowCursor {
    /// The live file this offset belongs to. `None` when there was no live file
    /// to read, in which case the follower waits for one to appear.
    identity: Option<(u64, u64)>,
    offset: u64,
}

#[cfg(unix)]
impl FollowCursor {
    /// A cursor for a service whose live file does not exist yet.
    #[must_use]
    pub fn pending() -> Self {
        Self {
            identity: None,
            offset: 0,
        }
    }
}

/// Reads the bounded log files one service wrote.
#[derive(Debug, Clone)]
pub struct ServiceLogReader {
    dir: PathBuf,
}

impl ServiceLogReader {
    #[must_use]
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
        }
    }

    /// The directory this reader reads.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.dir
    }

    /// The last `lines` lines the service wrote, oldest first, together with the
    /// point a follower should carry on from.
    ///
    /// Unix only, because the cursor identifies a file the way following does.
    #[cfg(unix)]
    ///
    /// The cursor is taken from the live file whether or not any history was
    /// asked for, so `-n 0 --follow` prints exactly the output that arrives
    /// after the command started, and `-n 200 --follow` prints no line twice.
    ///
    /// # Errors
    ///
    /// Returns an error when a file of the set exists but cannot be read. A
    /// file that is simply absent is not an error: the set is only as long as
    /// the service's output has made it.
    #[cfg(unix)]
    pub fn read_history(&self, lines: usize) -> io::Result<(Vec<String>, FollowCursor)> {
        let cursor = self.live_cursor()?;
        if lines == 0 {
            return Ok((Vec::new(), cursor));
        }

        // Newest file first, stopping as soon as enough lines are in hand, so a
        // small request never reads the whole retained set.
        //
        // The live file is read only as far as the cursor already taken, never
        // to its current end: the service is still writing, and a line appended
        // between the two reads would otherwise be printed once as history and
        // again as the first followed line.
        let mut collected: Vec<String> = Vec::new();
        let live = live_path(&self.dir);
        for path in rotation_set(&self.dir).into_iter().rev() {
            let remaining = lines - collected.len();
            let limit = if path == live {
                Some(cursor.offset)
            } else {
                None
            };
            let mut from_file = tail_lines(&path, remaining, limit)?;
            from_file.extend(collected);
            collected = from_file;
            if collected.len() >= lines {
                break;
            }
        }
        Ok((collected, cursor))
    }

    /// Where the live file currently ends.
    #[cfg(unix)]
    fn live_cursor(&self) -> io::Result<FollowCursor> {
        match std::fs::metadata(live_path(&self.dir)) {
            Ok(metadata) => Ok(FollowCursor {
                identity: Some(file_identity(&metadata)),
                offset: metadata.len(),
            }),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(FollowCursor::pending()),
            Err(e) => Err(e),
        }
    }

    /// Print every line written after `cursor`, until `keep_going` says to
    /// stop.
    ///
    /// Unix only: following depends on telling one file from another by
    /// identity, and no supervisor this crate serves runs elsewhere.
    ///
    /// Rotation is followed by identity rather than by path. The file the
    /// reader holds open is compared with the one on the live path on every
    /// poll; when they differ, the open file has been renamed down the set, so
    /// what is left of it is drained and the reader walks *forward* through the
    /// set — `spiced.log.2` to `spiced.log.1` to `spiced.log` — until it is
    /// back on the live file. Jumping straight to the live file instead would
    /// silently drop everything written between two polls that spanned a
    /// rotation.
    ///
    /// A live file that shrank below the read offset was truncated in place
    /// rather than renamed, and is read again from its start.
    ///
    /// # Errors
    ///
    /// Returns an error when a file of the set exists but cannot be read.
    #[cfg(unix)]
    pub fn follow<E, K>(
        &self,
        cursor: FollowCursor,
        mut emit: E,
        mut keep_going: K,
    ) -> io::Result<()>
    where
        E: FnMut(&str),
        K: FnMut() -> bool,
    {
        // A cursor that named a file resumes on that exact file wherever it has
        // since been renamed to, so output written between reading the history
        // and starting to follow is still printed.
        let mut open = match cursor.identity {
            Some(identity) => self
                .open_by_identity(identity)?
                .map(|file| (file, identity)),
            None => None,
        };
        let mut offset = cursor.offset;
        if open.is_none() {
            // A cursor whose file aged out of the set between taking it and
            // resuming starts at the oldest output still retained, for the same
            // reason as `open_successor`; one that never named a file starts
            // wherever the follower first finds one.
            if cursor.identity.is_some() {
                open = self.open_oldest_retained()?;
            }
            offset = 0;
        }
        let mut partial: Vec<u8> = Vec::new();

        while keep_going() {
            // One poll drains as far forward through the set as the writer has
            // moved, so a burst of rotations between two polls costs no output.
            for _ in 0..RETAINED_FILES {
                if open.is_none() {
                    open = self.open_live()?;
                    offset = 0;
                }
                let Some((file, identity)) = open.as_mut() else {
                    break;
                };

                if file.metadata()?.len() < offset {
                    offset = 0;
                    partial.clear();
                }
                let (consumed, at_eof) = drain(file, offset, &mut partial, &mut emit)?;
                offset += consumed;

                // Checked after draining, so the tail of a file that has just
                // been rotated away is printed before its successor is opened.
                // A file with unread bytes left keeps its turn: it is still the
                // one holding output nobody has seen.
                if !at_eof || self.is_live(*identity)? {
                    break;
                }
                if !partial.is_empty() {
                    emit(String::from_utf8_lossy(&partial).trim_end_matches('\r'));
                    partial.clear();
                }
                open = self.open_successor(*identity)?;
                offset = 0;
            }

            std::thread::sleep(FOLLOW_POLL_INTERVAL);
        }
        Ok(())
    }

    /// Whether `identity` is the file the live path names right now.
    #[cfg(unix)]
    fn is_live(&self, identity: (u64, u64)) -> io::Result<bool> {
        match std::fs::metadata(live_path(&self.dir)) {
            Ok(metadata) => Ok(file_identity(&metadata) == identity),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// The file of the set that currently holds `identity`, wherever rotation
    /// has moved it to. `None` when it has been rotated out of the set
    /// altogether, or the directory holds nothing yet.
    #[cfg(unix)]
    fn open_by_identity(&self, identity: (u64, u64)) -> io::Result<Option<File>> {
        for path in newest_first(&self.dir) {
            match File::open(&path) {
                Ok(file) => {
                    if file_identity(&file.metadata()?) == identity {
                        return Ok(Some(file));
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }

    /// The file written immediately after the one holding `identity`: its
    /// neighbour one place newer in the set, or the live file when `identity`
    /// has aged out of the set entirely.
    #[cfg(unix)]
    fn open_successor(&self, identity: (u64, u64)) -> io::Result<Option<(File, (u64, u64))>> {
        let paths = newest_first(&self.dir);
        let mut position = None;
        for (index, path) in paths.iter().enumerate() {
            match File::open(path) {
                Ok(file) => {
                    if file_identity(&file.metadata()?) == identity {
                        position = Some(index);
                        break;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        match position {
            // Index 0 is the live file, which has no successor.
            Some(0) => self.open_live(),
            Some(index) => match File::open(&paths[index - 1]) {
                Ok(file) => {
                    let found = file_identity(&file.metadata()?);
                    Ok(Some((file, found)))
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => self.open_live(),
                Err(e) => Err(e),
            },
            // The file being followed has been rotated out of the set
            // entirely. Its successor is then the oldest file still retained,
            // not the live one: jumping straight to live would discard every
            // rotation still on disk, which is exactly the output produced by
            // the burst the reader fell behind on.
            None => self.open_oldest_retained(),
        }
    }

    /// The oldest file the policy still retains, falling forward to the live
    /// file when none of the rotations exist.
    #[cfg(unix)]
    fn open_oldest_retained(&self) -> io::Result<Option<(File, (u64, u64))>> {
        for path in rotation_set(&self.dir) {
            match File::open(&path) {
                Ok(file) => {
                    let identity = file_identity(&file.metadata()?);
                    return Ok(Some((file, identity)));
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }

    /// The live file, or `None` when the service has written nothing yet.
    #[cfg(unix)]
    fn open_live(&self) -> io::Result<Option<(File, (u64, u64))>> {
        match File::open(live_path(&self.dir)) {
            Ok(file) => {
                let identity = file_identity(&file.metadata()?);
                Ok(Some((file, identity)))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// Every file of the policy, newest first: the live file, then its rotations.
#[cfg(unix)]
fn newest_first(dir: &Path) -> Vec<PathBuf> {
    let mut paths = rotation_set(dir);
    paths.reverse();
    paths
}

/// Read from `offset` to the end of `file`, emitting every complete line and
/// keeping any trailing fragment in `partial`.
///
/// Returns the bytes consumed and whether the end of the file was reached — a
/// read bounded by [`FOLLOW_READ_LIMIT`], so a writer faster than its reader
/// cannot make one poll unbounded, means the caller must come back for the
/// rest before moving on to another file.
#[cfg(unix)]
fn drain<E>(
    file: &mut File,
    offset: u64,
    partial: &mut Vec<u8>,
    emit: &mut E,
) -> io::Result<(u64, bool)>
where
    E: FnMut(&str),
{
    file.seek(SeekFrom::Start(offset))?;
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 8192];
    let mut consumed = 0_u64;
    let mut at_eof = false;
    while buffer.len() < FOLLOW_READ_LIMIT {
        let read = file.read(&mut chunk)?;
        if read == 0 {
            at_eof = true;
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        consumed += read as u64;
    }
    if buffer.is_empty() {
        return Ok((0, at_eof));
    }

    // The partial buffer holds *bytes*, and decoding happens one whole line at
    // a time. Decoding each poll's buffer instead would turn any multi-byte
    // character straddling the read limit into replacement markers on both
    // sides of the boundary — corrupting output that is perfectly valid on
    // disk. A complete line is a complete sequence, so lossy decoding of one is
    // only reached by bytes the writer really did emit.
    partial.extend_from_slice(&buffer);
    while let Some(end) = partial.iter().position(|byte| *byte == b'\n') {
        let line = String::from_utf8_lossy(&partial[..end]);
        emit(line.trim_end_matches('\r'));
        partial.drain(..=end);
    }
    Ok((consumed, at_eof))
}

/// The last `wanted` lines of one file, oldest first.
///
/// Read backwards in chunks so asking for a hundred lines of a ten-megabyte
/// file reads a few kilobytes rather than the whole file.
fn tail_lines(path: &Path, wanted: usize, limit: Option<u64>) -> io::Result<Vec<String>> {
    if wanted == 0 {
        return Ok(Vec::new());
    }
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    // `limit` is the live file's length as the follow cursor recorded it, so
    // history stops exactly where following will start.
    let length = match limit {
        Some(limit) => limit.min(file.metadata()?.len()),
        None => file.metadata()?.len(),
    };
    if length == 0 {
        return Ok(Vec::new());
    }

    let mut window = TAIL_CHUNK_BYTES.min(length);
    loop {
        let start = length - window;
        file.seek(SeekFrom::Start(start))?;
        let mut buffer = vec![0_u8; usize::try_from(window).unwrap_or(usize::MAX)];
        file.read_exact(&mut buffer)?;

        let text = String::from_utf8_lossy(&buffer);
        let mut lines: Vec<String> = text
            .split('\n')
            .map(|line| line.trim_end_matches('\r').to_string())
            .collect();
        // A file that ends in a newline produces a trailing empty element that
        // is not a line anyone wrote.
        if text.ends_with('\n') {
            lines.pop();
        }
        // The first element of a window that did not start at the beginning of
        // the file is a fragment of a line whose start was not read, so it is
        // only a whole line once the window reaches the file's start.
        if start > 0 && !lines.is_empty() {
            lines.remove(0);
        }

        if lines.len() >= wanted || start == 0 {
            let excess = lines.len().saturating_sub(wanted);
            return Ok(lines.split_off(excess));
        }
        window = (window + TAIL_CHUNK_BYTES).min(length);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_lines(log: &mut RotatingLog, lines: &[&str]) {
        for line in lines {
            log.write_all(format!("{line}\n").as_bytes())
                .expect("write a log line");
        }
        log.flush().expect("flush the log");
    }

    #[test]
    fn the_rotation_set_is_the_fixed_policy_oldest_first() {
        let dir = Path::new("/var/log/spice/edge");
        let set = rotation_set(dir);
        assert_eq!(set.len(), RETAINED_FILES);
        assert_eq!(set[0], dir.join("spiced.log.4"));
        assert_eq!(set[RETAINED_FILES - 1], dir.join("spiced.log"));
        assert_eq!(live_path(dir), dir.join("spiced.log"));
        assert_eq!(rotated_path(dir, 1), dir.join("spiced.log.1"));
    }

    #[test]
    fn rotation_never_keeps_more_files_than_the_policy() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut log = RotatingLog::open(dir.path()).expect("open the log");
        // Rotate by hand rather than by writing ten megabytes eight times over:
        // what is under test is the shifting of the set, not the byte counter.
        for round in 0..(RETAINED_FILES * 2) {
            write_lines(&mut log, &[&format!("round {round}")]);
            log.rotate().expect("rotate");
        }

        let present: Vec<PathBuf> = rotation_set(dir.path())
            .into_iter()
            .filter(|path| path.exists())
            .collect();
        assert_eq!(present.len(), RETAINED_FILES, "{present:?}");
        let stray = std::fs::read_dir(dir.path())
            .expect("read the log directory")
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| {
                !rotation_set(dir.path())
                    .iter()
                    .any(|path| path.file_name().and_then(|n| n.to_str()) == Some(name.as_str()))
            })
            .collect::<Vec<_>>();
        assert!(stray.is_empty(), "unexpected files left behind: {stray:?}");
    }

    #[test]
    fn a_write_past_the_bound_rotates_before_the_next_record() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut log = RotatingLog::open(dir.path()).expect("open the log");
        log.written = MAX_FILE_BYTES;
        write_lines(&mut log, &["after the bound"]);

        assert!(rotated_path(dir.path(), 1).exists());
        let live = std::fs::read_to_string(live_path(dir.path())).expect("read the live file");
        assert_eq!(live, "after the bound\n");
    }

    #[cfg(unix)]
    #[test]
    fn history_reads_back_across_rotated_files() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut log = RotatingLog::open(dir.path()).expect("open the log");
        write_lines(&mut log, &["one", "two"]);
        log.rotate().expect("rotate");
        write_lines(&mut log, &["three", "four"]);

        let reader = ServiceLogReader::new(dir.path());
        let (lines, _) = reader.read_history(10).expect("read history");
        assert_eq!(lines, vec!["one", "two", "three", "four"]);

        let (last_three, _) = reader.read_history(3).expect("read history");
        assert_eq!(last_three, vec!["two", "three", "four"]);
    }

    #[cfg(unix)]
    #[test]
    fn history_of_a_service_that_has_written_nothing_is_empty() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let reader = ServiceLogReader::new(dir.path());
        let (lines, cursor) = reader.read_history(100).expect("read history");
        assert!(lines.is_empty());
        assert_eq!(cursor, FollowCursor::pending());

        // An existing but empty live file is the same answer.
        let _log = RotatingLog::open(dir.path()).expect("open the log");
        let (lines, _) = reader.read_history(100).expect("read history");
        assert!(lines.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn a_line_without_a_terminator_is_still_history() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut log = RotatingLog::open(dir.path()).expect("open the log");
        log.write_all(b"finished\nunfinished").expect("write");
        log.flush().expect("flush");

        let (lines, _) = ServiceLogReader::new(dir.path())
            .read_history(10)
            .expect("read history");
        assert_eq!(lines, vec!["finished", "unfinished"]);
    }

    #[cfg(unix)]
    #[test]
    fn a_tail_longer_than_one_read_window_returns_whole_lines() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut log = RotatingLog::open(dir.path()).expect("open the log");
        let written: Vec<String> = (0..4000)
            .map(|i| format!("line {i} {}", "x".repeat(64)))
            .collect();
        for line in &written {
            log.write_all(format!("{line}\n").as_bytes())
                .expect("write");
        }
        log.flush().expect("flush");

        let (lines, _) = ServiceLogReader::new(dir.path())
            .read_history(1500)
            .expect("read history");
        assert_eq!(lines.len(), 1500);
        assert_eq!(lines[0], written[written.len() - 1500]);
        assert_eq!(lines[1499], written[written.len() - 1]);
    }

    /// Drive the follower on this thread by letting it run for a fixed number
    /// of polls, so a test never depends on a background thread's timing.
    #[cfg(unix)]
    fn follow_for(reader: &ServiceLogReader, cursor: FollowCursor, polls: usize) -> Vec<String> {
        let mut seen = Vec::new();
        let mut remaining = polls;
        reader
            .follow(
                cursor,
                |line| seen.push(line.to_string()),
                || {
                    let more = remaining > 0;
                    remaining = remaining.saturating_sub(1);
                    more
                },
            )
            .expect("follow the log");
        seen
    }

    #[cfg(unix)]
    #[test]
    fn following_from_a_cursor_prints_only_what_came_after_it() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut log = RotatingLog::open(dir.path()).expect("open the log");
        write_lines(&mut log, &["before"]);

        let reader = ServiceLogReader::new(dir.path());
        let (history, cursor) = reader.read_history(0).expect("read history");
        assert!(history.is_empty(), "-n 0 prints no history");

        write_lines(&mut log, &["after"]);
        assert_eq!(follow_for(&reader, cursor, 1), vec!["after"]);
    }

    #[cfg(unix)]
    #[test]
    fn following_survives_rotation_and_reopens_the_replacement() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut log = RotatingLog::open(dir.path()).expect("open the log");
        let reader = ServiceLogReader::new(dir.path());
        let (_, cursor) = reader.read_history(0).expect("read history");

        write_lines(&mut log, &["before rotation"]);
        log.rotate().expect("rotate");
        write_lines(&mut log, &["after rotation"]);

        assert_eq!(
            follow_for(&reader, cursor, 1),
            vec!["before rotation", "after rotation"]
        );
    }

    #[cfg(unix)]
    #[test]
    fn following_survives_several_rotations_between_polls() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut log = RotatingLog::open(dir.path()).expect("open the log");
        let reader = ServiceLogReader::new(dir.path());
        let (_, cursor) = reader.read_history(0).expect("read history");

        for round in 0..3 {
            write_lines(&mut log, &[&format!("round {round}")]);
            log.rotate().expect("rotate");
        }
        write_lines(&mut log, &["current"]);

        // One poll walks the whole way forward: three rotations between two
        // polls must cost no line, because the reader follows the file it holds
        // to its new name rather than jumping to whatever is live now.
        assert_eq!(
            follow_for(&reader, cursor, 1),
            vec!["round 0", "round 1", "round 2", "current"]
        );
    }

    #[cfg(unix)]
    #[test]
    fn following_reopens_a_file_that_was_truncated_in_place() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut log = RotatingLog::open(dir.path()).expect("open the log");
        write_lines(&mut log, &["a comfortably long first generation of output"]);
        let reader = ServiceLogReader::new(dir.path());
        let (_, cursor) = reader.read_history(0).expect("read history");

        // Truncation is detected by the file shrinking below the read offset —
        // the same signal `tail -f` uses. Nothing in this crate truncates a log
        // in place (rotation renames, and a restart appends), so this is an
        // outside actor emptying the file while a follower is on it.
        std::fs::write(live_path(dir.path()), b"second\n").expect("truncate in place");
        assert_eq!(follow_for(&reader, cursor, 1), vec!["second"]);
    }

    #[cfg(unix)]
    #[test]
    fn following_waits_for_a_service_that_has_not_written_yet() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let reader = ServiceLogReader::new(dir.path());
        let (_, cursor) = reader.read_history(0).expect("read history");
        assert_eq!(cursor, FollowCursor::pending());

        let mut log = RotatingLog::open(dir.path()).expect("open the log");
        write_lines(&mut log, &["the first line"]);
        assert_eq!(follow_for(&reader, cursor, 1), vec!["the first line"]);
    }

    #[cfg(unix)]
    #[test]
    fn a_restarted_runtime_appends_rather_than_starting_the_file_again() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut first = RotatingLog::open(dir.path()).expect("open the log");
        write_lines(&mut first, &["from the first run"]);
        drop(first);

        let mut second = RotatingLog::open(dir.path()).expect("reopen the log");
        write_lines(&mut second, &["from the second run"]);

        let (lines, _) = ServiceLogReader::new(dir.path())
            .read_history(10)
            .expect("read history");
        assert_eq!(lines, vec!["from the first run", "from the second run"]);
    }

    #[cfg(unix)]
    #[test]
    fn a_multibyte_character_survives_the_poll_read_limit() {
        // Decoding each poll's buffer independently turned any character
        // straddling the read limit into replacement markers on both sides of
        // it — corrupting output that is perfectly valid on disk. Decoding one
        // whole line at a time is what makes the boundary invisible.
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut log = RotatingLog::open(dir.path()).expect("open the log");

        // A line long enough to cross the follower's per-poll read limit, with
        // multi-byte characters all the way through it so a split is certain.
        let line: String = "日本語テキスト".repeat(FOLLOW_READ_LIMIT / 12);
        assert!(
            line.len() > FOLLOW_READ_LIMIT,
            "the line must cross the limit"
        );

        let reader = ServiceLogReader::new(dir.path());
        let (_, cursor) = reader.read_history(0).expect("take a cursor");
        write_lines(&mut log, &[&line]);

        let seen = follow_for(&reader, cursor, 4);
        assert_eq!(seen.len(), 1, "one line in, one line out");
        assert_eq!(seen[0], line, "the line must round-trip byte for byte");
        assert!(
            !seen[0].contains('\u{fffd}'),
            "no replacement markers may appear"
        );
    }

    #[cfg(unix)]
    #[test]
    fn history_is_bounded_by_the_cursor_it_is_returned_with() {
        // `read_history` takes the cursor first and then reads the live file
        // only that far, so a line the service appends between the two is not
        // in the history *and* is still after the cursor — which is what makes
        // it print exactly once, from the follower.
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut log = RotatingLog::open(dir.path()).expect("open the log");
        write_lines(&mut log, &["first", "second"]);
        let cursor_offset = std::fs::metadata(live_path(dir.path()))
            .expect("stat the live file")
            .len();

        // What the writer does after the cursor is taken.
        write_lines(&mut log, &["appended after the cursor"]);

        // Reading the live file bounded to the cursor must stop before it.
        let bounded = tail_lines(&live_path(dir.path()), 100, Some(cursor_offset))
            .expect("read the bounded tail");
        assert_eq!(
            bounded,
            vec!["first", "second"],
            "a bounded read must not see past the cursor"
        );
        // Unbounded, the same file does see it — so the bound is what matters.
        let unbounded =
            tail_lines(&live_path(dir.path()), 100, None).expect("read the unbounded tail");
        assert_eq!(unbounded.len(), 3);

        // End to end: the pair `read_history` returns never double-prints.
        let reader = ServiceLogReader::new(dir.path());
        let (history, cursor) = reader.read_history(100).expect("read history");
        write_lines(&mut log, &["after the history read"]);
        let followed = follow_for(&reader, cursor, 1);
        for line in &followed {
            assert!(
                !history.contains(line),
                "{line:?} was printed as history and again by the follower"
            );
        }
        assert_eq!(followed, vec!["after the history read"]);
    }

    #[cfg(unix)]
    #[test]
    fn a_follower_that_fell_behind_resumes_at_the_oldest_retained_file() {
        // When the followed file has been rotated out of the set entirely,
        // jumping to the live file would discard every rotation still on disk —
        // which is precisely the output the burst it fell behind on produced.
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut log = RotatingLog::open(dir.path()).expect("open the log");
        let reader = ServiceLogReader::new(dir.path());
        write_lines(&mut log, &["the file the cursor names"]);
        let (_, cursor) = reader.read_history(0).expect("take a cursor");

        // More rotations than the policy retains, so the cursor's own file is
        // gone from disk by the time the follower resumes.
        for round in 0..(RETAINED_FILES + 2) {
            log.rotate().expect("rotate");
            write_lines(&mut log, &[&format!("round {round}")]);
        }

        let seen = follow_for(&reader, cursor, 1);
        assert!(
            seen.len() > 1,
            "the retained rotations must still be printed, got {seen:?}"
        );
        assert!(
            seen.last().is_some_and(|line| line.contains("round")),
            "and it must end on the live file, got {seen:?}"
        );
    }

    #[test]
    fn a_record_never_lands_in_a_file_it_would_overflow() {
        // The bound is per file, and a record is never split, so a record that
        // does not fit starts a new file instead of overflowing the current one.
        let dir = tempfile::tempdir().expect("create tempdir");
        let mut log = RotatingLog::open(dir.path()).expect("open the log");
        log.written = MAX_FILE_BYTES - 16;
        write_lines(
            &mut log,
            &["a record that does not fit in the remaining sixteen bytes"],
        );

        assert!(
            rotated_path(dir.path(), 1).exists(),
            "the oversized record must have started a new file"
        );
        let live = std::fs::read_to_string(live_path(dir.path())).expect("read the live file");
        assert_eq!(
            live,
            "a record that does not fit in the remaining sixteen bytes\n"
        );
    }

    #[cfg(unix)]
    #[test]
    fn two_services_write_to_two_directories_and_never_mix() {
        let root = tempfile::tempdir().expect("create tempdir");
        let one = root.path().join("edge-1");
        let two = root.path().join("edge-2");
        let mut first = RotatingLog::open(&one).expect("open the first log");
        let mut second = RotatingLog::open(&two).expect("open the second log");
        write_lines(&mut first, &["only edge-1"]);
        write_lines(&mut second, &["only edge-2"]);

        let (first_lines, _) = ServiceLogReader::new(&one)
            .read_history(10)
            .expect("read the first history");
        let (second_lines, _) = ServiceLogReader::new(&two)
            .read_history(10)
            .expect("read the second history");
        assert_eq!(first_lines, vec!["only edge-1"]);
        assert_eq!(second_lines, vec!["only edge-2"]);
    }
}
