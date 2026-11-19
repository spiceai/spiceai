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

//! A buffered NFS read must refuse a length above the same cap the SMB store
//! uses, before any buffer of that length is allocated.
//!
//! The peer is an NFSv3 endpoint built from the reply layouts libnfs 5.0.2
//! decodes (`nfs_stat_64.nfs_size` from `GETATTR3`, then `nfs_pread`). It
//! accepts the mount with no server credential. The first `GETATTR` for
//! `huge.bin` reports `HUGE_SIZE`. A later `GETATTR` for the same handle on
//! that connection reports a 4-byte file, so a check that looks the file up
//! again and then buffers the second size still fails these tests.
//!
//! These tests fail while that allocation is uncapped.

#![cfg(feature = "nfs")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use datafusion::execution::object_store::ObjectStoreRegistry;
use libnfs::Nfs;
use nix::fcntl::OFlag;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use runtime_object_store::registry::SpiceObjectStoreRegistry;
use tokio::runtime::Handle;
use url::Url;

/// Same ceiling `SMBObjectStore` applies before buffering a read.
const MAX_BUFFERED_READ: u64 = 2 * 1024 * 1024 * 1024;
/// Server-reported length of `huge.bin` on the first `GETATTR`.
const HUGE_SIZE: u64 = MAX_BUFFERED_READ + 4096;
const SMALL_NAME: &str = "small.bin";
const HUGE_NAME: &str = "huge.bin";
const SMALL_BYTES: &[u8] = b"okay";
const ROOT_FH: &[u8] = b"root-fh!";
const SMALL_FH: &[u8] = b"small-fh";
const HUGE_FH: &[u8] = b"huge-fh!";

const PMAP_PROGRAM: u32 = 100_000;
const PMAP_V2: u32 = 2;
const PMAP2_NULL: u32 = 0;
const PMAP2_GETPORT: u32 = 3;
const MOUNT_PROGRAM: u32 = 100_005;
const MOUNT_V3: u32 = 3;
const MOUNT3_NULL: u32 = 0;
const MOUNT3_MNT: u32 = 1;
const MOUNT3_EXPORT: u32 = 5;
const NFS_PROGRAM: u32 = 100_003;
const NFS_V3: u32 = 3;
const NFS3_NULL: u32 = 0;
const NFS3_GETATTR: u32 = 1;
const NFS3_LOOKUP: u32 = 3;
const NFS3_ACCESS: u32 = 4;
const NFS3_READ: u32 = 6;
const NFS3_FSINFO: u32 = 19;
const IPPROTO_TCP: u32 = 6;
const NFS3_OK: u32 = 0;
const NFS3ERR_NOENT: u32 = 2;
const NF3REG: u32 = 1;
const NF3DIR: u32 = 2;
const LAST_FRAGMENT: u32 = 0x8000_0000;

static OVERSIZE_ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct RejectOversized;

fn note_allocation(size: usize) {
    if u64::try_from(size).unwrap_or(u64::MAX) > MAX_BUFFERED_READ {
        OVERSIZE_ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    }
}

fn refuse(size: usize) -> bool {
    note_allocation(size);
    u64::try_from(size).unwrap_or(u64::MAX) > MAX_BUFFERED_READ
}

// SAFETY: the allocator delegates to the system allocator for every request
// that fits in the buffered-read cap, and returns null for anything larger so
// the process aborts instead of committing that buffer.
unsafe impl GlobalAlloc for RejectOversized {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if refuse(layout.size()) {
            return std::ptr::null_mut();
        }
        // SAFETY: `layout` is the layout the caller asked the global allocator to serve.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if refuse(layout.size()) {
            return std::ptr::null_mut();
        }
        // SAFETY: `layout` is the layout the caller asked the global allocator to serve.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if refuse(new_size) {
            return std::ptr::null_mut();
        }
        // SAFETY: `ptr`/`layout` came from a previous allocation by this allocator.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr`/`layout` came from a previous allocation by this allocator.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: RejectOversized = RejectOversized;

fn oversize_allocations() -> usize {
    OVERSIZE_ALLOCATIONS.load(Ordering::Relaxed)
}

fn refusal_text(len: u64) -> String {
    format!("NFS read of {len} bytes exceeds {MAX_BUFFERED_READ}-byte cap")
}

fn assert_refused(err: &dyn std::fmt::Display, len: u64) {
    let text = err.to_string();
    let expected = refusal_text(len);
    assert!(
        text.contains(&expected),
        "refusal must name the buffered length and the cap, got {text}"
    );
    assert_eq!(
        oversize_allocations(),
        0,
        "the read allocated a buffer larger than {MAX_BUFFERED_READ} bytes before refusing"
    );
}

fn assert_within_cap() {
    assert_eq!(
        oversize_allocations(),
        0,
        "a read within the cap allocated a buffer larger than {MAX_BUFFERED_READ} bytes"
    );
}

fn ensure_server() {
    static SERVER: OnceLock<()> = OnceLock::new();
    SERVER.get_or_init(|| {
        let listener = match TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 111))) {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                bind_behind_portmapper_proxy()
            }
            Err(error) => panic!("bind 127.0.0.1:111: {error}"),
        };
        listener
            .set_nonblocking(false)
            .expect("the NFS listener is blocking");
        thread::spawn(move || accept_loop(listener));
    });
}

fn bind_behind_portmapper_proxy() -> TcpListener {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .expect("bind the local NFS port");
    let port = listener
        .local_addr()
        .expect("read the local NFS port")
        .port();
    let mut proxy = Command::new("sudo")
        .args(["-n", "python3", "-c", PROXY, &port.to_string()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start the port 111 proxy");
    let stdout = proxy.stdout.take().expect("proxy stdout is piped");
    let mut line = String::new();
    BufReader::new(stdout)
        .read_line(&mut line)
        .expect("read the proxy ready line");
    assert!(
        line.contains("bound"),
        "port 111 proxy did not bind, it printed {line:?}"
    );
    // Holding the child keeps its stdin open. The proxy exits on stdin EOF,
    // which happens when this process does, including an allocation abort.
    static PROXY_CHILD: Mutex<Option<Child>> = Mutex::new(None);
    *PROXY_CHILD.lock().expect("the proxy handle lock") = Some(proxy);
    listener
}

const PROXY: &str = r#"
import os, socket, sys, threading
dest = int(sys.argv[1])
ls = socket.socket()
ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
ls.bind(("127.0.0.1", 111))
ls.listen(128)
print("bound", flush=True)
def pump(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    for sock, how in ((src, socket.SHUT_RD), (dst, socket.SHUT_WR)):
        try:
            sock.shutdown(how)
        except OSError:
            pass
def accept():
    while True:
        client, _ = ls.accept()
        try:
            upstream = socket.create_connection(("127.0.0.1", dest))
        except OSError:
            client.close()
            continue
        threading.Thread(target=pump, args=(client, upstream), daemon=True).start()
        threading.Thread(target=pump, args=(upstream, client), daemon=True).start()
threading.Thread(target=accept, daemon=True).start()
try:
    while os.read(0, 1):
        pass
except OSError:
    pass
os._exit(0)
"#;

fn accept_loop(listener: TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept() else {
            break;
        };
        thread::spawn(move || {
            if let Err(error) = serve_connection(stream) {
                eprintln!("nfs test server connection closed: {error}");
            }
        });
    }
}

struct Conn {
    huge_getattr: u32,
}

fn serve_connection(mut stream: TcpStream) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let mut conn = Conn { huge_getattr: 0 };
    loop {
        let Some(record) = read_record(&mut stream)? else {
            return Ok(());
        };
        let Some(reply) = dispatch(&mut conn, &record) else {
            return Ok(());
        };
        stream.write_all(&reply)?;
    }
}

fn read_record(stream: &mut TcpStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut marker = [0_u8; 4];
    match stream.read_exact(&mut marker) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let mut body = Vec::new();
    loop {
        let raw = u32::from_be_bytes(marker);
        let len = usize::try_from(raw & !LAST_FRAGMENT).expect("fragment length fits");
        let start = body.len();
        body.resize(start + len, 0);
        stream.read_exact(&mut body[start..])?;
        if raw & LAST_FRAGMENT != 0 {
            break;
        }
        stream.read_exact(&mut marker)?;
    }
    Ok(Some(body))
}

struct Rd<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> Rd<'a> {
    fn u32(&mut self) -> Option<u32> {
        let end = self.at.checked_add(4)?;
        let bytes: [u8; 4] = self.buf.get(self.at..end)?.try_into().ok()?;
        self.at = end;
        Some(u32::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> Option<u64> {
        let high = u64::from(self.u32()?);
        let low = u64::from(self.u32()?);
        Some((high << 32) | low)
    }

    fn opaque(&mut self) -> Option<&'a [u8]> {
        let len = usize::try_from(self.u32()?).ok()?;
        let end = self.at.checked_add(len)?;
        let bytes = self.buf.get(self.at..end)?;
        self.at = end.checked_add(3)? & !3;
        Some(bytes)
    }
}

struct Wr {
    buf: Vec<u8>,
}

impl Wr {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    fn u32(&mut self, value: u32) {
        self.buf.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.u32(u32::try_from(value >> 32).expect("high half of a u64 fits in u32"));
        self.u32(u32::try_from(value & 0xffff_ffff).expect("low half of a u64 fits in u32"));
    }

    fn opaque(&mut self, bytes: &[u8]) {
        self.u32(u32::try_from(bytes.len()).expect("opaque length fits in u32"));
        self.buf.extend_from_slice(bytes);
        let pad = (4 - (bytes.len() % 4)) % 4;
        self.buf.resize(self.buf.len() + pad, 0);
    }

    fn finish(self, xid: u32) -> Vec<u8> {
        let mut frame = Vec::with_capacity(4 + 24 + self.buf.len());
        let marker = LAST_FRAGMENT
            | u32::try_from(24 + self.buf.len()).expect("reply length fits in the record marker");
        frame.extend_from_slice(&marker.to_be_bytes());
        frame.extend_from_slice(&xid.to_be_bytes());
        frame.extend_from_slice(&1_u32.to_be_bytes());
        frame.extend_from_slice(&0_u32.to_be_bytes());
        frame.extend_from_slice(&0_u32.to_be_bytes());
        frame.extend_from_slice(&0_u32.to_be_bytes());
        frame.extend_from_slice(&0_u32.to_be_bytes());
        frame.extend_from_slice(&self.buf);
        frame
    }
}

fn dispatch(conn: &mut Conn, record: &[u8]) -> Option<Vec<u8>> {
    let mut rd = Rd { buf: record, at: 0 };
    let xid = rd.u32()?;
    let direction = rd.u32()?;
    if direction != 0 {
        return None;
    }
    let _rpcvers = rd.u32()?;
    let prog = rd.u32()?;
    let vers = rd.u32()?;
    let proc = rd.u32()?;
    let _cred_flavor = rd.u32()?;
    let _cred = rd.opaque()?;
    let _verf_flavor = rd.u32()?;
    let _verf = rd.opaque()?;
    let body = reply_body(conn, prog, vers, proc, &mut rd);
    Some(body.finish(xid))
}

fn reply_body(conn: &mut Conn, prog: u32, vers: u32, proc: u32, rd: &mut Rd<'_>) -> Wr {
    let mut wr = Wr::new();
    match (prog, vers, proc) {
        (PMAP_PROGRAM, PMAP_V2, PMAP2_NULL)
        | (MOUNT_PROGRAM, MOUNT_V3, MOUNT3_NULL)
        | (NFS_PROGRAM, NFS_V3, NFS3_NULL) => {}
        (PMAP_PROGRAM, PMAP_V2, PMAP2_GETPORT) => {
            let program = rd.u32().unwrap_or(0);
            let _version = rd.u32().unwrap_or(0);
            let protocol = rd.u32().unwrap_or(0);
            let port = if protocol == IPPROTO_TCP
                && (program == MOUNT_PROGRAM || program == NFS_PROGRAM)
            {
                111
            } else {
                0
            };
            wr.u32(port);
        }
        (MOUNT_PROGRAM, MOUNT_V3, MOUNT3_MNT) => {
            wr.u32(0);
            wr.opaque(ROOT_FH);
            wr.u32(0);
        }
        (MOUNT_PROGRAM, MOUNT_V3, MOUNT3_EXPORT) => {
            wr.u32(0);
        }
        (NFS_PROGRAM, NFS_V3, NFS3_FSINFO) => put_fsinfo(&mut wr),
        (NFS_PROGRAM, NFS_V3, NFS3_GETATTR) => put_getattr(conn, &mut wr, rd),
        (NFS_PROGRAM, NFS_V3, NFS3_LOOKUP) => put_lookup(&mut wr, rd),
        (NFS_PROGRAM, NFS_V3, NFS3_ACCESS) => put_access(&mut wr, rd),
        (NFS_PROGRAM, NFS_V3, NFS3_READ) => put_read(&mut wr, rd),
        _ => {
            eprintln!("nfs test server has no handler for program {prog} procedure {proc}");
            wr.u32(NFS3ERR_NOENT);
        }
    }
    wr
}

fn put_fattr(wr: &mut Wr, kind: u32, size: u64, fileid: u64) {
    wr.u32(kind);
    wr.u32(0o644);
    wr.u32(1);
    wr.u32(0);
    wr.u32(0);
    wr.u64(size);
    wr.u64(size);
    wr.u32(0);
    wr.u32(0);
    wr.u64(1);
    wr.u64(fileid);
    for _ in 0..3 {
        wr.u32(1_700_000_000);
        wr.u32(0);
    }
}

fn put_post_attr(wr: &mut Wr, kind: u32, size: u64, fileid: u64) {
    wr.u32(1);
    put_fattr(wr, kind, size, fileid);
}

fn put_fsinfo(wr: &mut Wr) {
    wr.u32(NFS3_OK);
    wr.u32(0);
    for _ in 0..2 {
        wr.u32(65_536);
        wr.u32(65_536);
        wr.u32(4096);
    }
    wr.u32(65_536);
    wr.u64(u64::MAX);
    wr.u32(1);
    wr.u32(0);
    wr.u32(0);
}

fn file_of(fh: &[u8]) -> Option<FileKind> {
    if fh == ROOT_FH {
        Some(FileKind::Root)
    } else if fh == SMALL_FH {
        Some(FileKind::Small)
    } else if fh == HUGE_FH {
        Some(FileKind::Huge)
    } else {
        None
    }
}

enum FileKind {
    Root,
    Small,
    Huge,
}

fn put_getattr(conn: &mut Conn, wr: &mut Wr, rd: &mut Rd<'_>) {
    let Some(fh) = rd.opaque() else {
        wr.u32(NFS3ERR_NOENT);
        return;
    };
    let Some(kind) = file_of(fh) else {
        wr.u32(NFS3ERR_NOENT);
        return;
    };
    let (kind_tag, size, fileid) = match kind {
        FileKind::Root => (NF3DIR, 4096, 1),
        FileKind::Small => (NF3REG, u64::try_from(SMALL_BYTES.len()).expect("small file fits"), 2),
        FileKind::Huge => {
            conn.huge_getattr += 1;
            let size = if conn.huge_getattr == 1 {
                HUGE_SIZE
            } else {
                u64::try_from(SMALL_BYTES.len()).expect("second stat fits")
            };
            (NF3REG, size, 3)
        }
    };
    wr.u32(NFS3_OK);
    put_fattr(wr, kind_tag, size, fileid);
}

fn put_lookup(wr: &mut Wr, rd: &mut Rd<'_>) {
    let Some(_dir) = rd.opaque() else {
        wr.u32(NFS3ERR_NOENT);
        wr.u32(0);
        return;
    };
    let Some(name) = rd.opaque() else {
        wr.u32(NFS3ERR_NOENT);
        wr.u32(0);
        return;
    };
    let name = String::from_utf8_lossy(name);
    let (fh, size, fileid) = if name == SMALL_NAME {
        (SMALL_FH, u64::try_from(SMALL_BYTES.len()).expect("small file fits"), 2)
    } else if name == HUGE_NAME {
        (HUGE_FH, HUGE_SIZE, 3)
    } else {
        wr.u32(NFS3ERR_NOENT);
        wr.u32(0);
        return;
    };
    wr.u32(NFS3_OK);
    wr.opaque(fh);
    put_post_attr(wr, NF3REG, size, fileid);
    put_post_attr(wr, NF3DIR, 4096, 1);
}

fn put_access(wr: &mut Wr, rd: &mut Rd<'_>) {
    let Some(_fh) = rd.opaque() else {
        wr.u32(NFS3ERR_NOENT);
        wr.u32(0);
        return;
    };
    let access = rd.u32().unwrap_or(0);
    wr.u32(NFS3_OK);
    wr.u32(0);
    wr.u32(access);
}

fn put_read(wr: &mut Wr, rd: &mut Rd<'_>) {
    let Some(fh) = rd.opaque() else {
        wr.u32(NFS3ERR_NOENT);
        wr.u32(0);
        return;
    };
    let offset = rd.u64().unwrap_or(0);
    let count = rd.u32().unwrap_or(0);
    let (data, eof) = read_file(fh, offset, count);
    wr.u32(NFS3_OK);
    wr.u32(0);
    wr.u32(u32::try_from(data.len()).expect("read reply fits in count3"));
    wr.u32(u32::from(eof));
    wr.opaque(&data);
}

fn read_file(fh: &[u8], offset: u64, count: u32) -> (Vec<u8>, bool) {
    if fh == SMALL_FH {
        let start = usize::try_from(offset).unwrap_or(SMALL_BYTES.len());
        if start >= SMALL_BYTES.len() {
            return (Vec::new(), true);
        }
        let end = start
            .saturating_add(usize::try_from(count).unwrap_or(0))
            .min(SMALL_BYTES.len());
        return (SMALL_BYTES[start..end].to_vec(), end == SMALL_BYTES.len());
    }
    if fh == HUGE_FH {
        let count = u64::from(count).min(65_536);
        let mut data = Vec::with_capacity(usize::try_from(count).expect("chunk fits"));
        for index in 0..count {
            data.push(byte_at(offset + index));
        }
        let eof = offset.saturating_add(count) >= HUGE_SIZE;
        return (data, eof);
    }
    (Vec::new(), true)
}

fn byte_at(offset: u64) -> u8 {
    u8::try_from(offset % 251).expect("offset modulo 251 fits in a byte")
}

fn store() -> std::sync::Arc<dyn ObjectStore> {
    ensure_server();
    let registry = SpiceObjectStoreRegistry::new(Handle::current());
    let url = Url::parse("nfs://127.0.0.1/export").expect("the NFS URL is valid");
    registry
        .get_store(&url)
        .expect("the registry builds an NFS store for nfs://")
}

fn huge_path() -> ObjectPath {
    ObjectPath::from(HUGE_NAME)
}

fn small_path() -> ObjectPath {
    ObjectPath::from(SMALL_NAME)
}

async fn read_bytes(path: &ObjectPath, options: GetOptions) -> (u64, Vec<u8>) {
    let got = store()
        .get_opts(path, options)
        .await
        .expect("the ranged read is within the cap");
    let size = got.meta.size;
    let bytes = got.bytes().await.expect("the ranged body is readable");
    assert_within_cap();
    (size, bytes.to_vec())
}

fn open_huge() -> libnfs::NfsFile {
    ensure_server();
    let nfs = Nfs::new().expect("libnfs context");
    nfs.mount("127.0.0.1", "/export")
        .expect("mount the test export");
    nfs.open(std::path::Path::new("/huge.bin"), OFlag::O_RDONLY)
        .expect("open huge.bin")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_file_round_trip_reads_the_server_bytes() {
    let (size, bytes) = read_bytes(&small_path(), GetOptions::default()).await;
    assert_eq!(size, u64::try_from(SMALL_BYTES.len()).expect("small file fits"));
    assert_eq!(bytes, SMALL_BYTES);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_range_of_oversized_file_reads_only_that_range() {
    let (size, bytes) = read_bytes(
        &huge_path(),
        GetOptions {
            range: Some(GetRange::Bounded(0..4)),
            ..GetOptions::default()
        },
    )
    .await;
    assert!(size > MAX_BUFFERED_READ);
    assert_eq!(bytes, (0..4).map(byte_at).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn short_suffix_of_oversized_file_reads_only_that_suffix() {
    let suffix = 8_u64;
    let (size, bytes) = read_bytes(
        &huge_path(),
        GetOptions {
            range: Some(GetRange::Suffix(suffix)),
            ..GetOptions::default()
        },
    )
    .await;
    assert_eq!(size, HUGE_SIZE);
    let start = size - suffix;
    let expected = (0..suffix).map(|index| byte_at(start + index)).collect::<Vec<_>>();
    assert_eq!(bytes, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn head_of_oversized_file_reports_the_size_without_buffering_it() {
    let meta = store()
        .head(&huge_path())
        .await
        .expect("head of an oversized file is metadata only");
    assert_eq!(meta.size, HUGE_SIZE);
    let headed = store()
        .get_opts(
            &huge_path(),
            GetOptions {
                head: true,
                ..GetOptions::default()
            },
        )
        .await
        .expect("get_opts head of an oversized file is metadata only");
    assert_eq!(headed.meta.size, HUGE_SIZE);
    let body = headed.bytes().await.expect("head body");
    assert!(body.is_empty());
    assert_within_cap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_get_refuses_the_server_reported_length() {
    let error = store()
        .get(&huge_path())
        .await
        .expect_err("a full read of the server-reported length is refused");
    assert_refused(&error, HUGE_SIZE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_opts_without_a_range_refuses_the_server_reported_length() {
    let error = store()
        .get_opts(&huge_path(), GetOptions::default())
        .await
        .expect_err("get_opts with no range is refused");
    assert_refused(&error, HUGE_SIZE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_opts_from_the_start_refuses_the_remaining_length() {
    let error = store()
        .get_opts(
            &huge_path(),
            GetOptions {
                range: Some(GetRange::Offset(0)),
                ..GetOptions::default()
            },
        )
        .await
        .expect_err("an offset of zero still reads the whole reported length");
    assert_refused(&error, HUGE_SIZE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_opts_offset_inside_the_file_refuses_the_remaining_length() {
    let offset = 100_u64;
    let error = store()
        .get_opts(
            &huge_path(),
            GetOptions {
                range: Some(GetRange::Offset(offset)),
                ..GetOptions::default()
            },
        )
        .await
        .expect_err("the bytes after the offset are still over the cap");
    assert_refused(&error, HUGE_SIZE - offset);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_opts_suffix_over_the_cap_refuses_that_suffix() {
    let suffix = MAX_BUFFERED_READ + 1;
    let error = store()
        .get_opts(
            &huge_path(),
            GetOptions {
                range: Some(GetRange::Suffix(suffix)),
                ..GetOptions::default()
            },
        )
        .await
        .expect_err("a suffix longer than the cap is refused");
    assert_refused(&error, suffix);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_opts_bounded_range_over_the_cap_refuses_that_range() {
    let len = MAX_BUFFERED_READ + 1;
    let error = store()
        .get_opts(
            &huge_path(),
            GetOptions {
                range: Some(GetRange::Bounded(0..len)),
                ..GetOptions::default()
            },
        )
        .await
        .expect_err("a bounded range longer than the cap is refused");
    assert_refused(&error, len);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_range_over_the_cap_refuses_that_range() {
    let len = MAX_BUFFERED_READ + 1;
    let error = store()
        .get_range(&huge_path(), 0..len)
        .await
        .expect_err("get_range longer than the cap is refused");
    assert_refused(&error, len);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_ranges_over_the_cap_refuses_that_range() {
    let len = MAX_BUFFERED_READ + 1;
    let error = store()
        .get_ranges(&huge_path(), &[0..len])
        .await
        .expect_err("get_ranges longer than the cap is refused");
    assert_refused(&error, len);
}

#[test]
fn pread_refuses_a_count_over_the_cap() {
    let file = open_huge();
    let error = file
        .pread(HUGE_SIZE, 0)
        .expect_err("pread of the server-reported length is refused");
    assert_refused(&error, HUGE_SIZE);
}

#[test]
fn read_refuses_a_count_over_the_cap() {
    let file = open_huge();
    let error = file
        .read(HUGE_SIZE)
        .expect_err("read of the server-reported length is refused");
    assert_refused(&error, HUGE_SIZE);
}
