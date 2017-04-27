use fuse::{Filesystem, Request, ReplyAttr};
use time::Timespec;

use nix::libc::ENOSYS;

use catalog::Catalog;
use store::Store;

pub struct Fs<S> {
    catalog: Catalog,
    _store: S,
}

impl<S> Fs<S>
    where S: Store
{
    pub fn new(catalog: Catalog, store: S) -> Fs<S> {
        Fs {
            catalog: catalog,
            _store: store,
        }
    }
}

impl<S> Filesystem for Fs<S> {
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr(ino={})", ino);
        match self.catalog.get_inode(&ino) {
            Some(inode) => {
                let ttl = Timespec::new(1, 0);
                reply.attr(&ttl, &inode.attributes);
            }
            None => {
                reply.error(ENOSYS);
            }
        }
    }
}
