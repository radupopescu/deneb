use std::cell::RefCell;
use std::io::Result as IoResult;
use std::rc::Rc;
use std::sync::Arc;

use cas::Digest;
use errors::DenebResult;
use inode::{ChunkDescriptor, FileAttributes, INode};
use store::Store;

pub(crate) struct FileWorkspace<S> {
    attributes: FileAttributes,
    lower: Lower<S>,
}

impl<S> FileWorkspace<S>
where
    S: Store,
{
    pub(crate) fn new(inode: &INode, store: Rc<RefCell<S>>) -> FileWorkspace<S> {
        let lower = Lower::new(inode.chunks.as_slice(), store);
        FileWorkspace {
            attributes: inode.attributes,
            lower,
        }
    }

    pub(crate) fn read(&self, _offset: usize, _size: usize) -> IoResult<Vec<u8>> {
        Ok(vec![])
        /*chunks_to_buffer(
            &lookup_chunks(offset, size, inode.chunks.as_slice()),
            &self.store,
        )*/
    }

    pub(crate) fn unload(&mut self) -> IoResult<()> {
        self.lower.unload()
    }
}

struct Lower<S> {
    chunks: Vec<Chunk<S>>,
}

impl<S> Lower<S>
where
    S: Store,
{
    fn new(chunk_descriptors: &[ChunkDescriptor], store: Rc<RefCell<S>>) -> Lower<S> {
        let mut chunks = vec![];
        for &ChunkDescriptor { digest, size } in chunk_descriptors {
            chunks.push(Chunk::new(digest, size, Rc::clone(&store)));
        }
        Lower { chunks }
    }

    fn unload(&mut self) -> IoResult<()> {
        for c in self.chunks.iter_mut() {
            c.unload()?;
        }

        Ok(())
    }
}

struct Chunk<S> {
    digest: Digest,
    size: usize,
    store: Rc<RefCell<S>>,
    data: Option<Arc<Vec<u8>>>,
}

impl<S> Chunk<S>
where
    S: Store,
{
    fn new(digest: Digest, size: usize, store: Rc<RefCell<S>>) -> Chunk<S> {
        Chunk {
            digest,
            size,
            store,
            data: None,
        }
    }

    fn load(&mut self) -> DenebResult<()> {
        if self.data.is_none() {
            self.data = Some(self.store.borrow().get_chunk(&self.digest)?);
        }
        Ok(())
    }

    fn unload(&mut self) -> IoResult<()> {
        if self.data.is_some() {
            self.data = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use store::MemStore;
    use util::run;

    #[test]
    fn try_file_workspace() {
        run(|| {
            let store = Rc::new(RefCell::new(MemStore::new(10000)));

            let names = ["ala", "bala", "portocala"];
            let mut chunks = vec![];
            for n in names.iter() {
                chunks.push(store.borrow_mut().put_file(n.as_bytes())?);
            }
            let inode = INode {
                attributes: FileAttributes::default(),
                chunks,
            };
            let mut ws = FileWorkspace::new(&inode, Rc::clone(&store));

            ws.unload()?;

            Ok(())
        });
    }
}
