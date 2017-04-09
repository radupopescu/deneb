use fuse::{FileAttr, FileType, Filesystem, Request, ReplyAttr};
use time::Timespec;

use nix::libc::ENOSYS;

use catalog::Catalog;
use store::Store;

pub struct Fs<H, S> {
    catalog: Catalog<H>,
    store: S,
}

impl<H, S> Fs<H, S> {
    pub fn new(catalog: Catalog<H>, store: S) -> Fs<H, S> {
        Fs { catalog: catalog, store: store }
    }
}

impl<H, S> Filesystem for Fs<H, S> {
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
