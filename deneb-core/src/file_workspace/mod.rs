use std::cell::RefCell;
use std::cmp::min;
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

    pub(crate) fn read(&mut self, offset: usize, size: usize) -> DenebResult<Vec<u8>> {
        let chunk_parts = lookup_chunks(offset, size, self.lower.chunks.as_slice());
        let buffer = self.fill_buffer(&chunk_parts)?;
        Ok(buffer)
    }

    pub(crate) fn unload(&mut self) {
        self.lower.unload();
    }

    fn fill_buffer(&mut self, chunks: &[ChunkPart]) -> DenebResult<Vec<u8>> {
        let mut buffer = vec![];
        for &ChunkPart { index, begin, end } in chunks {
            let chunk = &mut self.lower.chunks[index];
            let slice = chunk.get_slice()?;
            buffer.extend_from_slice(&slice[begin..end]);
        }
        Ok(buffer)
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

    fn unload(&mut self) {
        for c in self.chunks.iter_mut() {
            c.unload();
        }
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

    fn unload(&mut self) {
        if self.data.is_some() {
            self.data = None;
        }
    }

    fn get_slice(&mut self) -> DenebResult<&[u8]> {
        self.load()?;
        // Note: The following unwrap should never panic
        Ok(self.data.as_ref().unwrap().as_slice())
    }
}

/// Data structure returned by the `lookup_chunks` function
///
/// The index identifying a chunk and the indices which define an
/// exclusive range of that should be read from the chunk data.
#[derive(Debug, PartialEq)]
struct ChunkPart {
    index: usize,
    begin: usize,
    end: usize
}

/// Lookup a subset of consecutive chunks corresponding to a memory slice
///
/// Given a list of `ChunkDescriptor`, representing consecutive chunks
/// of a file and a segment identified by `offset` - the offset from
/// the beginning of the file - and `size` - the size of the segment,
/// this function returns a vector of `ChunkPart`
fn lookup_chunks<S: Store>(offset: usize, size: usize, chunks: &[Chunk<S>]) -> Vec<ChunkPart> {
    let (first_chunk, mut offset_in_chunk) = chunk_idx_for_offset(offset, chunks);
    let mut output = Vec::new();
    let mut bytes_left = size;
    for (index, c) in chunks[first_chunk..].iter().enumerate() {
        let read_bytes = min(bytes_left, c.size - offset_in_chunk);
        output.push(ChunkPart {
            index: first_chunk + index,
            begin: offset_in_chunk,
            end: offset_in_chunk + read_bytes,
        });
        offset_in_chunk = 0;
        bytes_left -= read_bytes;
        if bytes_left == 0 {
            break;
        }
    }
    output
}

/// Lookup the index in a list of chunks corresponding to an offset
///
/// Returns a pair of `usize` representing the index of the chunk inside the list (slice)
/// and the offset inside the chunk which correspond to the given offset
fn chunk_idx_for_offset<S: Store>(offset: usize, chunks: &[Chunk<S>]) -> (usize, usize) {
    let mut acc = 0;
    let mut idx = 0;
    let mut offset_in_chunk = 0;
    for (i, c) in chunks.iter().enumerate() {
        acc += c.size;
        idx = i;
        if acc > offset {
            offset_in_chunk = offset + c.size - acc;
            break;
        }
    }
    (idx, offset_in_chunk)
}

#[cfg(test)]
mod tests {
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

            let res = ws.read(0, 17)?;

            assert_eq!(b"alabalaportocala", res.as_slice());

            ws.unload();

            Ok(())
        });
    }

    fn make_chunks<S: Store>(input_size: usize, chunk_size: usize, store: Rc<RefCell<S>>) -> Vec<Chunk<S>> {
        use cas::read_chunks;

        let input = (0..)
            .map(|e| (e as u64 % 256) as u8)
            .take(input_size)
            .collect::<Vec<u8>>();

        let mut buffer = vec![0 as u8; chunk_size];
        let raw_chunks = read_chunks(input.as_slice(), &mut buffer);
        assert!(raw_chunks.is_ok());
        let mut chunks = vec![];
        if let Ok(cs) = raw_chunks {
            for (digest, data) in cs {
                chunks.push(Chunk::new(digest, data.len(), Rc::clone(&store)));
            }
        }
        chunks
    }

    #[test]
    fn locate_slice_in_chunks() {
        let store = Rc::new(RefCell::new(MemStore::new(10000)));

        let chunks = make_chunks(20, 5, store);

        assert_eq!((0, 3), chunk_idx_for_offset(3, &chunks));
        assert_eq!((1, 2), chunk_idx_for_offset(7, &chunks));
        assert_eq!((2, 2), chunk_idx_for_offset(12, &chunks));
        assert_eq!((3, 0), chunk_idx_for_offset(15, &chunks));

        // Read 7 bytes starting at offset 6
        let offset = 6;
        let size = 7;

        let output = lookup_chunks(offset, size, &chunks);
        assert_eq!(2, output.len());
        assert_eq!(ChunkPart{ index: 1, begin: 1, end: 5 }, output[0]);
        assert_eq!(ChunkPart{ index: 2, begin: 0, end: 3 }, output[1]);

        // Read 11 bytes starting at offset 2
        let offset = 2;
        let size = 11;

        let output = lookup_chunks(offset, size, &chunks);
        assert_eq!(3, output.len());
        assert_eq!(ChunkPart{ index: 0, begin: 2, end: 5 }, output[0]);
        assert_eq!(ChunkPart{ index: 1, begin: 0, end: 5 }, output[1]);
        assert_eq!(ChunkPart{ index: 2, begin: 0, end: 3 }, output[2]);

        // Read 3 bytes starting at offset 12
        let offset = 12;
        let size = 3;

        let output = lookup_chunks(offset, size, &chunks);
        assert_eq!(1, output.len());
        assert_eq!(ChunkPart{ index: 2, begin: 2, end: 5 }, output[0]);

        // Read 100 bytes starting at offset 18 (should read to the end)
        let offset = 18;
        let size = 100;

        let output = lookup_chunks(offset, size, &chunks);
        assert_eq!(1, output.len());
        assert_eq!(ChunkPart{ index: 3, begin: 3, end: 5 }, output[0]);
    }
}
