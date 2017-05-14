use fuse::{Filesystem, Request, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
           ReplyOpen};
use fuse::consts::FOPEN_KEEP_CACHE;
use nix::libc::{O_WRONLY, O_RDWR};
use nix::libc::{EINVAL, EACCES};
use time::Timespec;

use std::ffi::OsStr;
use std::collections::HashMap;
use std::path::PathBuf;

use be::catalog::Catalog;
use be::store::Store;

struct OpenFileContext;

pub struct Fs<C, S> {
    catalog: C,
    store: S,

    open_dirs: HashMap<u64, Vec<(PathBuf, u64)>>,
    open_files: HashMap<u64, OpenFileContext>,
}

impl<C, S> Fs<C, S>
{
    pub fn new(catalog: C, store: S) -> Fs<C, S> {
        Fs {
            catalog: catalog,
            store: store,
            open_dirs: HashMap::new(),
            open_files: HashMap::new(),
        }
    }
}

impl<C, S> Filesystem for Fs<C, S>
    where C: Catalog, S: Store
{
    // Filesystem lifetime callbacks

    // fn init(&mut self, _req: &Request) -> Result<(), c_int> { Ok(()) }

    // fn destroy(&mut self, _req: &Request) { }


    // Callbacks for read-only functionality

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr(ino={})", ino);
        match self.catalog.get_inode(&ino) {
            Some(inode) => {
                let ttl = Timespec::new(1, 0);
                reply.attr(&ttl, &inode.attributes);
            }
            None => {
                reply.error(EINVAL);
            }
        }
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup(parent={}, name={:?}", parent, name);
        let attrs = self.catalog
            .get_dir_entries(&parent)
            .and_then(|entries| entries.get(&PathBuf::from(name)))
            .and_then(|index| self.catalog.get_inode(index))
            .map(|inode| inode.attributes);
        match attrs {
            Some(attrs) => {
                let ttl = Timespec::new(1, 0);
                reply.entry(&ttl, &attrs, 0);
            }
            None => {
                reply.error(EINVAL);
            }
        }
    }

    fn opendir(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        debug!("opendir - ino: {}", ino);
        match self.catalog.get_dir_entries(&ino) {
            Some(entries) => {
                // TODO: This copying is quite wasteful. Maybe improve with Rc<...>?
                let mut es = Vec::new();
                for (path, index) in entries.iter() {
                    es.push((path.to_owned(), *index));
                }
                self.open_dirs.insert(ino, es);
                reply.opened(ino, flags & !FOPEN_KEEP_CACHE);
            }
            None => {
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
               offset: u64,
               mut reply: ReplyDirectory) {
        debug!("readdir - ino: {}, fh: {}, offset: {}", ino, fh, offset);
        let mut index = offset as usize;
        match self.open_dirs.get(&fh) {
            Some(entries) => {
                while index < entries.len() {
                    let (ref name, idx) = entries[index];
                    if let Some(inode) = self.catalog.get_inode(&idx) {
                        if !reply.add(idx, index as u64 + 1, inode.attributes.kind, name) {
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
            match self.catalog.get_inode(&ino) {
                Some(_) => {
                    self.open_files.insert(ino, OpenFileContext);
                    reply.opened(ino, flags & !FOPEN_KEEP_CACHE);
                }
                None => {
                    reply.error(EINVAL);
                }
            }
        }
    }

    fn read(&mut self,
            _req: &Request,
            ino: u64,
            fh: u64,
            offset: u64,
            size: u32,
            reply: ReplyData) {
        debug!("read - ino: {}, fh: {}, offset: {}, size: {}",
               ino,
               fh,
               offset,
               size);
        let blob = self.open_files.get(&fh)
            .and_then(|_ctx| self.catalog.get_inode(&fh))
            .and_then(|inode| {
                let digests = &inode.digests;
                if !digests.is_empty() {
                    self.store.get(&digests[0])
                } else {
                    None
                }
            });
        match blob {
            Some(blob) => {
                let begin = offset as usize;
                let end = begin + size as usize;
                reply.data(&blob[begin..end]);
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
