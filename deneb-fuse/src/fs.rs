use fuse::{FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
           ReplyEntry, ReplyOpen, Request};
use fuse::consts::FOPEN_KEEP_CACHE;
use fuse::{mount, spawn_mount, BackgroundSession};
use nix::libc::{EACCES, EINVAL, ENOENT};
use time::Timespec;

use std::ffi::OsStr;
use std::path::Path;

use deneb_core::errors::{print_error_with_causes, CatalogError, DenebResult, EngineError};
use deneb_core::engine::{Handle, RequestId};
use deneb_core::inode::{FileAttributes, FileType as FT};

pub struct Session<'a>(BackgroundSession<'a>);

pub struct Fs {
    engine_handle: Handle,
}

impl<'a> Fs {
    pub fn new(engine_handle: Handle) -> Fs {
        Fs { engine_handle }
    }

    pub fn mount<P: AsRef<Path>>(self, mount_point: &P, options: &[&OsStr]) -> DenebResult<()> {
        mount(self, mount_point, options).map_err(|e| e.into())
    }

    pub unsafe fn spawn_mount<P: AsRef<Path>>(
        self,
        mount_point: &P,
        options: &[&OsStr],
    ) -> DenebResult<Session<'a>> {
        spawn_mount(self, mount_point, options)
            .map(Session)
            .map_err(|e| e.into())
    }
}

impl Filesystem for Fs {
    // Filesystem lifetime callbacks

    // fn init(&mut self, _req: &Request) -> Result<(), c_int> { Ok(()) }

    // fn destroy(&mut self, _req: &Request) { }

    // Callbacks for read-only functionality

    fn getattr(&mut self, req: &Request, ino: u64, reply: ReplyAttr) {
        match self.engine_handle.get_attr(&to_request_id(req), ino) {
            Ok(attrs) => {
                let ttl = Timespec::new(1, 0);
                reply.attr(&ttl, &to_fuse_file_attr(attrs));
            }
            Err(e) => {
                print_error_with_causes(&e);
                reply.error(EINVAL);
            }
        }
    }

    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.engine_handle
            .lookup(&to_request_id(req), parent, name)
        {
            Ok(attrs) => {
                let ttl = Timespec::new(1, 0);
                reply.entry(&ttl, &to_fuse_file_attr(attrs), 0);
            }
            Err(e) => {
                if let Some(engine_error) = e.root_cause().downcast_ref::<CatalogError>() {
                    if let &CatalogError::DEntryNotFound(..) = engine_error {
                        reply.error(ENOENT);
                        return;
                    }
                }
                print_error_with_causes(&e);
                reply.error(EINVAL);
            }
        }
    }

    fn opendir(&mut self, req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        match self.engine_handle
            .open_dir(&to_request_id(req), ino, flags)
        {
            Ok(()) => {
                reply.opened(ino, flags & !FOPEN_KEEP_CACHE);
            }
            Err(e) => {
                print_error_with_causes(&e);
                reply.error(EINVAL);
            }
        }
    }

    fn releasedir(&mut self, req: &Request, _ino: u64, fh: u64, flags: u32, reply: ReplyEmpty) {
        match self.engine_handle
            .release_dir(&to_request_id(req), fh, flags)
        {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => {
                print_error_with_causes(&e);
                reply.error(EINVAL);
            }
        }
    }

    fn readdir(
        &mut self,
        req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        match self.engine_handle
            .read_dir(&to_request_id(req), fh, offset)
        {
            Ok(entries) => {
                let mut index = ::std::cmp::max(offset, 0) as usize;
                while index < entries.len() {
                    let (ref name, idx, ftype) = entries[index];
                    if !reply.add(idx, index as i64 + 1, to_fuse_file_type(ftype), name) {
                        index += 1;
                    } else {
                        break;
                    }
                }
                reply.ok();
            }
            Err(e) => {
                print_error_with_causes(&e);
                reply.error(EINVAL);
            }
        }
    }

    fn open(&mut self, req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        match self.engine_handle
            .open_file(&to_request_id(req), ino, flags)
        {
            Ok(_) => {
                reply.opened(ino, flags & !FOPEN_KEEP_CACHE);
            },
            Err(e) => {
                if let Some(engine_error) = e.downcast_ref::<EngineError>() {
                    match engine_error {
                        &EngineError::Access(_) => {
                            reply.error(EACCES);
                        },
                        _ => {
                            print_error_with_causes(&e);
                            reply.error(EINVAL);
                        },
                    }
                }
            }
        }
    }

    fn read(&mut self, req: &Request, _ino: u64, fh: u64, offset: i64, size: u32, reply: ReplyData) {
        match self.engine_handle
            .read_data(&to_request_id(req), fh, offset, size)
        {
            Ok(buffer) => {
                reply.data(&buffer);
            }
            Err(e) => {
                print_error_with_causes(&e);
                reply.error(EINVAL);
            }
        }
    }

    fn release(
        &mut self,
        req: &Request,
        _ino: u64,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.engine_handle
            .release_file(&to_request_id(req), fh, flags, lock_owner, flush)
        {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => {
                print_error_with_causes(&e);
                reply.error(EINVAL);
            }
        }
    }

    /*
    fn readlink(&mut self, _req: &Request, _ino: u64, reply: ReplyData) {}

    fn access(&mut self, _req: &Request, _ino: u64, _mask: u32, reply: ReplyEmpty) {}

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {}

    fn getxattr(&mut self,
                _req: &Request,
                _ino: u64,
                _name: &OsStr,
                _size: u32,
                reply: ReplyXattr) {
    }
    fn listxattr(&mut self, _req: &Request, _ino: u64, _size: u32, reply: ReplyXattr) {}
    fn getlk(&mut self,
             _req: &Request,
             _ino: u64,
             _fh: u64,
             _lock_owner: u64,
             _start: u64,
             _end: u64,
             _typ: u32,
             _pid: u32,
             reply: ReplyLock) {
    }

    // Callbacks for write functionality
    fn forget(&mut self, _req: &Request, _ino: u64, _nlookup: u64) {}
    fn setattr(&mut self,
               _req: &Request,
               _ino: u64,
               _mode: Option<u32>,
               _uid: Option<u32>,
               _gid: Option<u32>,
               _size: Option<u64>,
               _atime: Option<Timespec>,
               _mtime: Option<Timespec>,
               _fh: Option<u64>,
               _crtime: Option<Timespec>,
               _chgtime: Option<Timespec>,
               _bkuptime: Option<Timespec>,
               _flags: Option<u32>,
               reply: ReplyAttr) {
    }
    fn mknod(&mut self,
             _req: &Request,
             _parent: u64,
             _name: &OsStr,
             _mode: u32,
             _rdev: u32,
             reply: ReplyEntry) {
    }
    fn mkdir(&mut self,
             _req: &Request,
             _parent: u64,
             _name: &OsStr,
             _mode: u32,
             reply: ReplyEntry) {
    }
    fn unlink(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {}
    fn rmdir(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {}
    fn symlink(&mut self,
               _req: &Request,
               _parent: u64,
               _name: &OsStr,
               _link: &Path,
               reply: ReplyEntry) {
    }
    fn rename(&mut self,
              _req: &Request,
              _parent: u64,
              _name: &OsStr,
              _newparent: u64,
              _newname: &OsStr,
              reply: ReplyEmpty) {
    }
    fn link(&mut self,
            _req: &Request,
            _ino: u64,
            _newparent: u64,
            _newname: &OsStr,
            reply: ReplyEntry) {
    }
    fn write(&mut self,
             _req: &Request,
             _ino: u64,
             _fh: u64,
             _offset: u64,
             _data: &[u8],
             _flags: u32,
             reply: ReplyWrite) {
    }
    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {}
    fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {}
    fn fsyncdir(&mut self,
                _req: &Request,
                _ino: u64,
                _fh: u64,
                _datasync: bool,
                reply: ReplyEmpty) {
    }
    fn setxattr(&mut self,
                _req: &Request,
                _ino: u64,
                _name: &OsStr,
                _value: &[u8],
                _flags: u32,
                _position: u32,
                reply: ReplyEmpty) {
    }
    fn removexattr(&mut self, _req: &Request, _ino: u64, _name: &OsStr, reply: ReplyEmpty) {}
    fn create(&mut self,
              _req: &Request,
              _parent: u64,
              _name: &OsStr,
              _mode: u32,
              _flags: u32,
              reply: ReplyCreate) {
    }
    fn setlk(&mut self,
             _req: &Request,
             _ino: u64,
             _fh: u64,
             _lock_owner: u64,
             _start: u64,
             _end: u64,
             _typ: u32,
             _pid: u32,
             _sleep: bool,
             reply: ReplyEmpty) {
    }

    // Other callbacks
    fn bmap(&mut self, _req: &Request, _ino: u64, _blocksize: u32, _idx: u64, reply: ReplyBmap) {}
     */
}

fn to_fuse_file_type(ftype: FT) -> FileType {
    match ftype {
        FT::NamedPipe => FileType::NamedPipe,
        FT::CharDevice => FileType::CharDevice,
        FT::BlockDevice => FileType::BlockDevice,
        FT::Directory => FileType::Directory,
        FT::RegularFile => FileType::RegularFile,
        FT::Symlink => FileType::Symlink,
    }
}

fn to_fuse_file_attr(fattr: FileAttributes) -> FileAttr {
    FileAttr {
        ino: fattr.ino,
        size: fattr.size,
        blocks: fattr.blocks,
        atime: fattr.atime,
        mtime: fattr.mtime,
        ctime: fattr.ctime,
        crtime: fattr.crtime,
        kind: to_fuse_file_type(fattr.kind),
        perm: fattr.perm,
        nlink: fattr.nlink,
        uid: fattr.uid,
        gid: fattr.gid,
        rdev: fattr.rdev,
        flags: fattr.flags,
    }
}

fn to_request_id(req: &Request) -> RequestId {
    RequestId {
        unique_id: req.unique(),
        uid: req.uid(),
        gid: req.gid(),
        pid: req.pid(),
    }
}

