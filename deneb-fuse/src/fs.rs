#[cfg(target_os = "linux")]
use nix::mount::{umount2, MntFlags};
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
use nix::{
    libc::{unmount, MNT_FORCE},
    NixPath,
};
use {
    deneb_core::{
        engine::{Handle, RequestId},
        errors::{print_error_with_causes, DenebResult, EngineError, UnixError},
        inode::{FileAttributeChanges, FileAttributes, FileType as FT},
    },
    fuse::{
        mount, spawn_mount, BackgroundSession, FileAttr, FileType, Filesystem, ReplyAttr,
        ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite,
        Request,
    },
    nix::libc::{EACCES, EINVAL, ENOENT},
    std::{
        ffi::OsStr,
        iter::Iterator,
        path::{Path, PathBuf},
    },
    time::Timespec,
};

pub struct Session<'a> {
    fuse_session: BackgroundSession<'a>,
    mount_point: PathBuf,
}

impl<'a> Session<'a> {
    pub fn new<P: AsRef<Path>>(
        fuse_session: BackgroundSession<'a>,
        mount_point: &P,
    ) -> Session<'a> {
        Session {
            fuse_session,
            mount_point: mount_point.as_ref().to_owned(),
        }
    }

    #[cfg(target_os = "linux")]
    pub fn force_unmount(self) -> Result<(), UnixError> {
        drop(self.fuse_session);
        umount2(self.mount_point.as_path(), MntFlags::MNT_FORCE)?;
        Ok(())
    }
    #[cfg(any(target_os = "macos", target_os = "freebsd"))]
    pub fn force_unmount(self) -> Result<(), UnixError> {
        drop(self.fuse_session);
        let _ = self
            .mount_point
            .as_path()
            .with_nix_path(|cstr| unsafe { unmount(cstr.as_ptr(), MNT_FORCE) })?;
        Ok(())
    }
}

pub struct Fs {
    engine_handle: Handle,
}

impl<'a> Fs {
    pub fn spawn_mount<P: AsRef<Path>>(
        mount_point: &P,
        engine_handle: Handle,
        options: &[String],
    ) -> DenebResult<Session<'a>> {
        let opts = options.iter().map(|o| o.as_ref()).collect::<Vec<&OsStr>>();
        let fs = Fs { engine_handle };
        unsafe {
            spawn_mount(fs, mount_point, &opts)
                .map(|s| Session::new(s, mount_point))
                .map_err(|e| e.into())
        }
    }

    pub fn mount<P: AsRef<Path>>(
        mount_point: &P,
        engine_handle: Handle,
        options: &[String],
    ) -> DenebResult<()> {
        let opts = options.iter().map(|o| o.as_ref()).collect::<Vec<&OsStr>>();
        let fs = Fs { engine_handle };
        mount(fs, mount_point, &opts).map_err(|e| e.into())
    }

    pub fn make_options(opts: &[String]) -> Vec<String> {
        opts.iter()
            .flat_map(|o| vec!["-o".to_owned(), o.clone()])
            .collect::<Vec<String>>()
    }
}

impl Filesystem for Fs {
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

    fn setattr(
        &mut self,
        req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
        _fh: Option<u64>,
        crtime: Option<Timespec>,
        chgtime: Option<Timespec>,
        _bkuptime: Option<Timespec>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let changes =
            FileAttributeChanges::new(mode, uid, gid, size, atime, mtime, crtime, chgtime, flags);
        match self
            .engine_handle
            .set_attr(&to_request_id(req), ino, changes)
        {
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
        match self.engine_handle.lookup(&to_request_id(req), parent, name) {
            Ok(Some(attrs)) => {
                let ttl = Timespec::new(1, 0);
                reply.entry(&ttl, &to_fuse_file_attr(attrs), 0);
            }
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(e) => {
                print_error_with_causes(&e);
                reply.error(EINVAL);
            }
        }
    }

    fn opendir(&mut self, req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        match self.engine_handle.open_dir(&to_request_id(req), ino, flags) {
            Ok(()) => {
                reply.opened(ino, 0);
            }
            Err(e) => {
                print_error_with_causes(&e);
                reply.error(EINVAL);
            }
        }
    }

    fn releasedir(&mut self, req: &Request, _ino: u64, fh: u64, flags: u32, reply: ReplyEmpty) {
        match self
            .engine_handle
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
        match self.engine_handle.read_dir(&to_request_id(req), fh, offset) {
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
        match self
            .engine_handle
            .open_file(&to_request_id(req), ino, flags)
        {
            Ok(_) => {
                reply.opened(ino, 0);
            }
            Err(e) => {
                if let Some(engine_error) = e.downcast_ref::<EngineError>() {
                    match *engine_error {
                        EngineError::Access(_) => {
                            reply.error(EACCES);
                        }
                        _ => {
                            print_error_with_causes(&e);
                            reply.error(EINVAL);
                        }
                    }
                }
            }
        }
    }

    fn read(
        &mut self,
        req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        match self
            .engine_handle
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

    fn write(
        &mut self,
        req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        match self
            .engine_handle
            .write_data(&to_request_id(req), fh, offset, data)
        {
            Ok(num_bytes_written) => {
                reply.written(num_bytes_written);
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
        match self
            .engine_handle
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

    fn create(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
        reply: ReplyCreate,
    ) {
        match self
            .engine_handle
            .create_file(&to_request_id(req), parent, name, mode, flags)
        {
            Ok((ino, attr)) => {
                let ttl = Timespec::new(1, 0);
                reply.created(&ttl, &to_fuse_file_attr(attr), 0, ino, 0);
            }
            Err(e) => {
                print_error_with_causes(&e);
                reply.error(EINVAL);
            }
        }
    }

    fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        match self
            .engine_handle
            .create_dir(&to_request_id(req), parent, name, mode)
        {
            Ok(attr) => {
                let ttl = Timespec::new(1, 0);
                reply.entry(&ttl, &to_fuse_file_attr(attr), 0);
            }
            Err(e) => {
                print_error_with_causes(&e);
                reply.error(EINVAL);
            }
        }
    }

    fn unlink(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.engine_handle.unlink(&to_request_id(req), parent, name) {
            Ok(()) => {
                reply.ok();
            }
            Err(e) => {
                print_error_with_causes(&e);
                reply.error(EINVAL);
            }
        }
    }

    fn rmdir(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self
            .engine_handle
            .remove_dir(&to_request_id(req), parent, name)
        {
            Ok(()) => {
                reply.ok();
            }
            Err(e) => {
                print_error_with_causes(&e);
                reply.error(EINVAL);
            }
        }
    }

    fn rename(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        reply: ReplyEmpty,
    ) {
        match self
            .engine_handle
            .rename(&to_request_id(req), parent, name, new_parent, new_name)
        {
            Ok(()) => {
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

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {}

    fn mknod(&mut self,
             _req: &Request,
             _parent: u64,
             _name: &OsStr,
             _mode: u32,
             _rdev: u32,
             reply: ReplyEntry) {
    }

    fn symlink(&mut self,
               _req: &Request,
               _parent: u64,
               _name: &OsStr,
               _link: &Path,
               reply: ReplyEntry) {
    }

    fn link(&mut self,
            _req: &Request,
            _ino: u64,
            _newparent: u64,
            _newname: &OsStr,
            reply: ReplyEntry) {
    }
    */

    /*
    // Other callbacks

    fn init(&mut self, _req: &Request) -> Result<(), c_int> { Ok(()) }
    fn destroy(&mut self, _req: &Request) { }

    fn access(&mut self, _req: &Request, _ino: u64, _mask: u32, reply: ReplyEmpty) {}

    fn getxattr(&mut self,
                _req: &Request,
                _ino: u64,
                _name: &OsStr,
                _size: u32,
                reply: ReplyXattr) {
    }
    fn listxattr(&mut self, _req: &Request, _ino: u64, _size: u32, reply: ReplyXattr) {}
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

    fn forget(&mut self, _req: &Request, _ino: u64, _nlookup: u64) {}

    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {}
    fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {}
    fn fsyncdir(&mut self,
                _req: &Request,
                _ino: u64,
                _fh: u64,
                _datasync: bool,
                reply: ReplyEmpty) {
    }

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
        ino: fattr.index,
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
