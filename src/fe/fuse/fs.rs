use fuse::{FileAttr, Filesystem, FileType, Request, ReplyAttr, ReplyData, ReplyDirectory,
           ReplyEmpty, ReplyEntry, ReplyOpen};
use fuse::consts::FOPEN_KEEP_CACHE;
use fuse::{BackgroundSession, mount, spawn_mount};
use nix::libc::{O_WRONLY, O_RDWR};
use nix::libc::{EINVAL, EACCES};
use time::Timespec;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use be::catalog::Catalog;
use be::inode::{ChunkPart, FileAttributes, FileType as FT, lookup_chunks};
use be::store::Store;
use common::errors::DenebResult;

struct OpenFileContext;

pub struct Session<'a>(BackgroundSession<'a>);

pub struct Fs<C, S> {
    catalog: C,
    store: S,

    open_dirs: HashMap<u64, Vec<(PathBuf, u64)>>,
    open_files: HashMap<u64, OpenFileContext>,
}

impl<'a, C, S> Fs<C, S>
    where C: 'a + Catalog + Send,
          S: 'a + Store + Send
{
    pub fn new(catalog: C, store: S) -> Fs<C, S> {
        Fs {
            catalog: catalog,
            store: store,
            open_dirs: HashMap::new(),
            open_files: HashMap::new(),
        }
    }

    pub fn mount<P: AsRef<Path>>(self, mount_point: &P, options: &[&OsStr]) -> DenebResult<()> {
        mount(self, mount_point, options).map_err(|e| e.into())
    }

    pub unsafe fn spawn_mount<P: AsRef<Path>>(self,
                                              mount_point: &P,
                                              options: &[&OsStr])
                                              -> DenebResult<Session<'a>> {
        spawn_mount(self, mount_point, options)
            .map(Session)
            .map_err(|e| e.into())
    }
}

impl<C, S> Filesystem for Fs<C, S>
    where C: Catalog,
          S: Store
{
    // Filesystem lifetime callbacks

    // fn init(&mut self, _req: &Request) -> Result<(), c_int> { Ok(()) }

    // fn destroy(&mut self, _req: &Request) { }


    // Callbacks for read-only functionality

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr(ino={})", ino);
        match self.catalog.get_inode(ino) {
            Ok(inode) => {
                let ttl = Timespec::new(1, 0);
                reply.attr(&ttl, &convert_fuse_fattr(&inode.attributes));
            }
            Err(_) => {
                reply.error(EINVAL);
            }
        }
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup(parent={}, name={:?}", parent, name);
        let attrs = self.catalog
            .get_dir_entry_inode(parent, PathBuf::from(name).as_path())
            .map(|inode| inode.attributes);
        match attrs {
            Ok(attrs) => {
                let ttl = Timespec::new(1, 0);
                reply.entry(&ttl, &convert_fuse_fattr(&attrs), 0);
            }
            Err(_) => {
                reply.error(EINVAL);
            }
        }
    }

    fn opendir(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        debug!("opendir - ino: {}", ino);
        match self.catalog.get_dir_entries(ino) {
            Ok(entries) => {
                self.open_dirs.insert(ino, entries);
                reply.opened(ino, flags & !FOPEN_KEEP_CACHE);
            }
            Err(_) => {
                reply.error(EINVAL);
            }
        }
    }

    fn releasedir(&mut self, _req: &Request, ino: u64, fh: u64, _flags: u32, reply: ReplyEmpty) {
        debug!("releasedir - ino: {}", ino);
        match self.open_dirs.remove(&fh) {
            Some(_) => {
                reply.ok();
            }
            None => {
                reply.error(EINVAL);
            }
        }
    }

    fn readdir(&mut self,
               _req: &Request,
               ino: u64,
               fh: u64,
               offset: i64,
               mut reply: ReplyDirectory) {
        debug!("readdir - ino: {}, fh: {}, offset: {}", ino, fh, offset);
        let mut index = ::std::cmp::max(offset, 0) as usize;
        match self.open_dirs.get(&fh) {
            Some(entries) => {
                while index < entries.len() {
                    let (ref name, idx) = entries[index];
                    if let Ok(inode) = self.catalog.get_inode(idx) {
                        if !reply.add(idx,
                                      index as i64 + 1,
                                      convert_fuse_file_type(inode.attributes.kind),
                                      name) {
                            index += 1;
                        } else {
                            break;
                        }
                    }
                }
                reply.ok();
            }
            None => {
                reply.error(EINVAL);
            }
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        let rw = (O_WRONLY | O_RDWR) as u32;
        if (flags & rw) > 0 {
            // If write access is requested, the function should return EACCES
            debug!("open RW - ino: {}", ino);
            reply.error(EACCES);
        } else {
            debug!("open RO - ino: {}", ino);
            match self.catalog.get_inode(ino) {
                Ok(_) => {
                    self.open_files.insert(ino, OpenFileContext);
                    reply.opened(ino, flags & !FOPEN_KEEP_CACHE);
                }
                Err(_) => {
                    reply.error(EINVAL);
                }
            }
        }
    }

    fn read(&mut self,
            _req: &Request,
            ino: u64,
            fh: u64,
            offset: i64,
            size: u32,
            reply: ReplyData) {
        debug!("read - ino: {}, fh: {}, offset: {}, size: {}",
               ino,
               fh,
               offset,
               size);
        let offset = ::std::cmp::max(offset, 0) as usize;
        let buffer = self.open_files
            .get(&fh)
            .and_then(|_ctx| self.catalog.get_inode(fh).ok())
            .and_then(|inode| {
                lookup_chunks(offset, size as usize, inode.chunks.as_slice())
                    .and_then(|chunks| {
                        chunks_to_buffer(chunks.as_slice(), &self.store).ok()
                    })
            });
        match buffer {
            Some(buffer) => {
                reply.data(buffer.as_slice());
            }
            None => {
                reply.error(EINVAL);
            }
        }
    }

    fn release(&mut self,
               _req: &Request,
               ino: u64,
               fh: u64,
               _flags: u32,
               _lock_owner: u64,
               _flush: bool,
               reply: ReplyEmpty) {
        debug!("release - ino: {}", ino);
        self.open_files.remove(&fh);
        reply.ok();
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

/// Fill a buffer using the list of `ChunkPart`
fn chunks_to_buffer<S: Store>(chunks: &[ChunkPart], store: &S) -> DenebResult<Vec<u8>> {
    let mut buffer = Vec::new();
    for &ChunkPart(digest, begin, end) in chunks {
        let chunk = store.get_chunk(digest)?;
        buffer.extend_from_slice(&chunk[begin..end]);
    }
    Ok(buffer)
}

fn convert_fuse_file_type(ftype: FT) -> FileType {
    match ftype {
        FT::NamedPipe => FileType::NamedPipe,
        FT::CharDevice => FileType::CharDevice,
        FT::BlockDevice => FileType::BlockDevice,
        FT::Directory => FileType::Directory,
        FT::RegularFile => FileType::RegularFile,
        FT::Symlink => FileType::Symlink,
    }
}

fn convert_fuse_fattr(fattr: &FileAttributes) -> FileAttr {
    FileAttr {
        ino: fattr.ino,
        size: fattr.size,
        blocks: fattr.blocks,
        atime: fattr.atime,
        mtime: fattr.mtime,
        ctime: fattr.ctime,
        crtime: fattr.crtime,
        kind: convert_fuse_file_type(fattr.kind),
        perm: fattr.perm,
        nlink: fattr.nlink,
        uid: fattr.uid,
        gid: fattr.gid,
        rdev: fattr.rdev,
        flags: fattr.flags,
    }
}
