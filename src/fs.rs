use fuse::{FileAttr, FileType, Filesystem, Request, ReplyAttr};
use time::Timespec;

use nix::libc::ENOSYS;

use catalog::Catalog;
use store::Store;

pub struct Fs<S> {
    catalog: Catalog,
    store: S,
}

impl<S> Fs<S> where S: Store {
    pub fn new(catalog: Catalog, store: S) -> Fs<S> {
        Fs { catalog: catalog, store: store }
    }
}

impl<S> Filesystem for Fs<S> {
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        info!("getattr(ino={})", ino);
        let ts = Timespec::new(0, 0);
        let attr = FileAttr {
            ino: 1,
            size: 0,
            blocks: 0,
            atime: ts,
            mtime: ts,
            ctime: ts,
            crtime: ts,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
        };
        let ttl = Timespec::new(1, 0);
        if ino == 1 {
            reply.attr(&ttl, &attr);
        } else {
            reply.error(ENOSYS);
        }
    }
}
