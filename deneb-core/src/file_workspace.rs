use std::{cell::RefCell, cmp::min, collections::HashMap, rc::Rc, sync::Arc};

use {
    cas::Digest,
    errors::DenebResult,
    inode::{ChunkDescriptor, INode},
    store::{Chunk, Store},
};

/// A type which offers read/write operations on a file in the repository
///
/// A `FileWorkspace` represents a superposition of a lower layer,
/// made up of immutable file chunks, and an upper layer storing the
/// modifications applied to the lower layer. It is similar in concept
/// to a union file-system such as Aufs or OverlayFS, only limited to the
/// scope of a single file.
///
/// Its implementation is based on interior mutability - caching of
/// unpackaged chunks in the lower layer is done transparently to the
/// client of the `FileWorkspace`.
pub(crate) struct FileWorkspace {
    lower: RefCell<Lower>,
    upper: Vec<u8>,
    piece_table: Vec<Piece>,
    size: u64,
}

impl FileWorkspace {
    /// Create a new `FileWorkspace` for an `INode`
    ///
    /// Constructs a new workspace object for the file described by
    /// `inode`. The function takes a reference-counted pointer to a
    /// `Store` object which is used by the underlying `Chunks` making
    /// up the lower, immutable, layer
    pub(crate) fn new(
        inode: &INode,
        store: &Rc<RefCell<Box<dyn Store>>>,
    ) -> DenebResult<FileWorkspace> {
        let lower = Lower::new(inode.chunks.as_slice(), store)?;
        let piece_table = inode
            .chunks
            .iter()
            .enumerate()
            .map(|(idx, c)| {
                let chunk_size = c.size;
                Piece {
                    target: PieceTarget::Lower(idx),
                    offset: 0,
                    size: chunk_size,
                }
            })
            .collect::<Vec<_>>();
        trace!(
            "New workspace for inode {} - size: {}, num_chunks: {}",
            inode.attributes.index,
            inode.attributes.size,
            lower.chunks.len()
        );
        Ok(FileWorkspace {
            lower: RefCell::new(lower),
            upper: vec![],
            piece_table,
            size: inode.attributes.size,
        })
    }

    /// Read `size` number of bytes, starting at `offset`
    pub(crate) fn read_at(&self, offset: usize, size: usize) -> DenebResult<Vec<u8>> {
        let slices = lookup_pieces(offset, size, &self.piece_table);
        let buffer = self.fill_buffer(&slices)?;
        Ok(buffer)
    }

    /// Truncate the workspace to a new size
    pub(crate) fn truncate(&mut self, new_size: u64) {
        if new_size == self.size {
            return;
        }

        if new_size == 0 {
            self.size = 0;
            self.piece_table.clear();
            self.upper.clear();
            return;
        }

        if new_size < self.size {
            let (piece_idx, offset_in_piece) =
                piece_idx_for_offset(new_size as usize, &self.piece_table);
            self.piece_table.truncate(piece_idx + 1);
            self.piece_table[piece_idx].size = offset_in_piece;
        } else {
            let extra_size = (new_size - self.size) as usize;
            self.piece_table.push(Piece {
                target: PieceTarget::Zero,
                offset: 0,
                size: extra_size,
            });
        }
        self.size = new_size;
    }

    /// Write the contents of buffer into the workspace, starting at `offset`
    ///
    /// Write the buffer into the workspace at `offset`, returning a tuple with the number of bytes
    /// written and the new file size
    pub(crate) fn write_at(&mut self, offset: usize, buffer: &[u8]) -> (u32, u64) {
        // Append buffer to the upper layer
        let buf_size = buffer.len();
        let offset_in_upper = self.upper.len();
        self.upper.extend_from_slice(buffer);

        let new_piece = Piece {
            target: PieceTarget::Upper,
            offset: offset_in_upper,
            size: buf_size,
        };

        // Corner cases: writing into an empty file or appending to the file
        if self.piece_table.is_empty() || (offset as u64 >= self.size) {
            if offset as u64 > self.size {
                self.piece_table.push(Piece {
                    target: PieceTarget::Zero,
                    offset: 0,
                    size: offset - self.size as usize,
                });
            }
            self.piece_table.push(new_piece);
            self.size = (offset + buf_size) as u64;
            return (buf_size as u32, self.size);
        }

        // Find the piece where the buffer is to be placed
        let (first_piece_idx, offset_in_first_piece) =
            piece_idx_for_offset(offset, &self.piece_table);

        // How many original pieces are kept in the new piece_table?
        let keep_idx = if offset_in_first_piece == 0 {
            first_piece_idx
        } else {
            first_piece_idx + 1
        };
        let mut new_piece_table = self.piece_table[..keep_idx].to_vec();
        if offset_in_first_piece != 0 {
            new_piece_table[keep_idx - 1].size = offset_in_first_piece;
        }

        // Add the new piece
        new_piece_table.push(new_piece);

        // Corner case: the buffer to be written extends to the end of the file
        //              or beyond it
        if (offset + buf_size) as u64 >= self.size {
            self.size = (offset + buf_size) as u64;
            self.piece_table = new_piece_table;
            return (buf_size as u32, self.size);
        }

        // Find the last piece touched by the buffer
        let (last_piece_idx, offset_in_last_piece) =
            piece_idx_for_offset(offset + buf_size, &self.piece_table);

        // Append the last relevant pieces from the original piece table and
        // adjust the first appended piece, as needed
        let save_idx = new_piece_table.len();
        new_piece_table.extend_from_slice(&self.piece_table[last_piece_idx..]);
        new_piece_table[save_idx].offset += offset_in_last_piece;
        new_piece_table[save_idx].size -= offset_in_last_piece;

        // Replace the old piece table with the new one
        self.piece_table = new_piece_table;

        (buf_size as u32, self.size)
    }

    /// Unload the lower layer from memory
    ///
    /// Forces the lower layer of the workspace to be unloaded from
    /// memory, when "closing" the workspace is desired, while
    /// maintaining any changes recorded in the top layer.
    pub(crate) fn unload(&self) {
        self.lower.borrow_mut().unload();
    }

    fn fill_buffer(&self, slices: &[PieceSlice]) -> DenebResult<Vec<u8>> {
        let mut buffer = vec![];
        for &PieceSlice { index, begin, end } in slices {
            let piece = &self.piece_table[index];
            match piece.target {
                PieceTarget::Lower(chunk_index) => {
                    let mut lower = self.lower.borrow_mut();
                    lower.load_chunk(chunk_index)?;
                    let mut chunk = &lower.chunks[&chunk_index];
                    let slice = chunk.get_slice();
                    buffer.extend_from_slice(&slice[(piece.offset + begin)..(piece.offset + end)]);
                }
                PieceTarget::Upper => {
                    buffer.extend_from_slice(
                        &self.upper[(piece.offset + begin)..(piece.offset + end)],
                    );
                }
                PieceTarget::Zero => {
                    buffer.append(&mut vec![0; piece.size]);
                }
            }
        }
        Ok(buffer)
    }
}

/// Target of the piece, either the lower or the upper layer of the workspace
#[derive(Clone)]
enum PieceTarget {
    /// The index represents which chunk of the lower layer this piece is related to
    Lower(usize),
    Upper,
    Zero,
}

/// A piece represents a subset of either the lower or upper layers
///
/// If a piece points the lower layer, and index is provided which identifies which
/// chunk in the lower layer is referenced.
#[derive(Clone)]
struct Piece {
    /// Target of piece
    target: PieceTarget,
    /// Offset of the beginning of the piece into the target buffer
    offset: usize,
    /// Size of the piece
    size: usize,
}

/// A slice of a `Piece`
#[derive(Debug, PartialEq)]
struct PieceSlice {
    /// The index of the associated `Piece` in the piece_table
    index: usize,
    /// Start position of the slice relative to the beginning of the piece
    begin: usize,
    /// End position of the slice relative to the beginning of the piece
    end: usize,
}

/// The lower, immutable, layer of a `FileWorkspace` object
///
/// The lower layer represents a vector of file `Chunk` objects. Each
/// chunk is wrapped in a `RefCell`, to allow certain mutable
/// operations on the chunks.
struct Lower {
    digests: Vec<Digest>,
    store: Rc<RefCell<Box<dyn Store>>>,
    chunks: HashMap<usize, Arc<dyn Chunk>>,
}

impl Lower {
    /// Construct the lower layer using a provided list of `ChunkDescriptor`
    fn new(
        chunk_descriptors: &[ChunkDescriptor],
        store: &Rc<RefCell<Box<dyn Store>>>,
    ) -> DenebResult<Lower> {
        let digests = chunk_descriptors
            .iter()
            .map(|&ChunkDescriptor { digest, .. }| digest)
            .collect::<Vec<_>>();
        Ok(Lower {
            digests,
            chunks: HashMap::new(),
            store: Rc::clone(store),
        })
    }

    // Load a single chunk
    #[cfg_attr(feature = "cargo-clippy", allow(map_entry))]
    fn load_chunk(&mut self, index: usize) -> DenebResult<()> {
        let digest = self.digests[index];
        if !self.chunks.contains_key(&index) {
            let chunk = self.store.borrow().get_chunk(&digest)?;
            self.chunks.insert(index, chunk);
        }
        Ok(())
    }

    /// Release the chunks that make up the lower layer
    fn unload(&mut self) {
        self.chunks.clear();
    }
}

/// Lookup a subset of pieces corresponding to a memory slice
///
/// Given a piece table and a segment identified by `offset` - the
/// offset from the beginning of the file - and `size` - the size of
/// the segment, this function returns a vector of `PieceSlice`
fn lookup_pieces(offset: usize, size: usize, piece_table: &[Piece]) -> Vec<PieceSlice> {
    let (first_piece, mut offset_in_piece) = piece_idx_for_offset(offset, piece_table);
    let mut output = Vec::new();
    let mut bytes_left = size;
    for (index, pc) in piece_table[first_piece..].iter().enumerate() {
        let read_bytes = min(bytes_left, pc.size - offset_in_piece);
        output.push(PieceSlice {
            index: first_piece + index,
            begin: offset_in_piece,
            end: offset_in_piece + read_bytes,
        });
        offset_in_piece = 0;
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
fn piece_idx_for_offset(offset: usize, piece_table: &[Piece]) -> (usize, usize) {
    let mut acc = 0;
    let mut idx = 0;
    let mut offset_in_piece = 0;
    for (i, pc) in piece_table.iter().enumerate() {
        acc += pc.size;
        idx = i;
        if acc > offset {
            offset_in_piece = offset + pc.size - acc;
            break;
        }
    }
    (idx, offset_in_piece)
}

#[cfg(test)]
mod tests {
    use super::*;

    use inode::FileAttributes;
    use store::{Builder, StoreType};

    fn make_test_workspace() -> DenebResult<FileWorkspace> {
        let mut store = Builder::create(StoreType::InMemory, "/", 10000)?;

        let mut names: Vec<&[u8]> = vec![b"ala", b"bala", b"portocala"];
        let mut chunks = vec![];
        for n in &mut names {
            chunks.push(store.put_file(n)?);
        }
        let mut attributes = FileAttributes::default();
        attributes.size = 16;
        let inode = INode { attributes, chunks };
        FileWorkspace::new(&inode, &Rc::new(RefCell::new(store)))
    }

    #[test]
    fn read() -> DenebResult<()> {
        let ws = make_test_workspace()?;

        let res = ws.read_at(0, ws.size as usize)?;

        assert_eq!(b"alabalaportocala", res.as_slice());

        ws.unload();

        Ok(())
    }

    #[test]
    fn write_into_empty() -> DenebResult<()> {
        let store = Builder::create(StoreType::InMemory, "/", 10000)?;

        let inode = INode {
            attributes: FileAttributes::default(),
            chunks: vec![],
        };
        let mut ws = FileWorkspace::new(&inode, &Rc::new(RefCell::new(store)))?;

        assert_eq!(ws.write_at(0, b"written"), (7, 7));

        let res = ws.read_at(0, 7)?;
        assert_eq!(b"written", res.as_slice());
        assert_eq!(ws.piece_table.len(), 1);
        assert_eq!(ws.size, 7);

        Ok(())
    }

    #[test]
    fn successive_writes() -> DenebResult<()> {
        let mut ws = make_test_workspace()?;

        let res0 = ws.read_at(0, 16)?;
        assert_eq!(b"alabalaportocala", res0.as_slice());

        assert_eq!(ws.write_at(2, b"written"), (7, 16));

        let res1 = ws.read_at(0, 16)?;
        assert_eq!(b"alwrittenrtocala", res1.as_slice());

        assert_eq!(ws.write_at(6, b"again"), (5, 16));

        ws.unload();

        let res2 = ws.read_at(0, 16)?;
        assert_eq!(b"alwritagainocala", res2.as_slice());

        Ok(())
    }

    #[test]
    fn write_at_beginning() -> DenebResult<()> {
        let mut ws = make_test_workspace()?;

        assert_eq!(ws.write_at(0, b"written"), (7, 16));

        let res = ws.read_at(0, 16)?;

        assert_eq!(b"writtenportocala", res.as_slice());
        assert_eq!(ws.piece_table.len(), 2);
        assert_eq!(ws.size, 16);

        Ok(())
    }

    #[test]
    fn write_at_end() -> DenebResult<()> {
        let mut ws = make_test_workspace()?;

        assert_eq!(ws.write_at(9, b"written"), (7, 16));

        let res = ws.read_at(0, 16)?;

        assert_eq!(b"alabalapowritten", res.as_slice());
        assert_eq!(ws.piece_table.len(), 4);
        assert_eq!(ws.size, 16);

        Ok(())
    }

    #[test]
    fn write_extends_the_file() -> DenebResult<()> {
        let mut ws = make_test_workspace()?;

        assert_eq!(ws.write_at(12, b"written"), (7, 19));

        let res = ws.read_at(0, 19)?;

        assert_eq!(b"alabalaportowritten", res.as_slice());
        assert_eq!(ws.piece_table.len(), 4);
        assert_eq!(ws.size, 19);

        Ok(())
    }

    #[test]
    fn append_to_file() -> DenebResult<()> {
        let mut ws = make_test_workspace()?;

        assert_eq!(ws.write_at(16, b"written"), (7, 23));

        let res = ws.read_at(0, 23)?;

        assert_eq!(b"alabalaportocalawritten", res.as_slice());
        assert_eq!(ws.piece_table.len(), 4);
        assert_eq!(ws.size, 23);

        Ok(())
    }

    #[test]
    fn write_beyond_end() -> DenebResult<()> {
        let mut ws = make_test_workspace()?;

        assert_eq!(ws.write_at(20, b"written"), (7, 27));

        let res = ws.read_at(0, 27)?;

        assert_eq!(
            [
                97, 108, 97, 98, 97, 108, 97, 112, 111, 114, 116, 111, 99, 97, 108, 97, 0, 0, 0, 0,
                119, 114, 105, 116, 116, 101, 110,
            ],
            res.as_slice()
        );
        assert_eq!(ws.piece_table.len(), 5);
        assert_eq!(ws.size, 27);

        Ok(())
    }

    #[test]
    fn locate_slice() {
        let input_size = 20;
        let chunk_size = 5;
        let piece_table = {
            let mut remaining_size = input_size;
            let mut pieces = vec![];
            while remaining_size > 0 {
                let size = min(remaining_size, chunk_size);
                remaining_size -= size;
                pieces.push(Piece {
                    target: PieceTarget::Lower(0),
                    offset: 0,
                    size,
                });
            }
            pieces
        };

        assert_eq!((0, 0), piece_idx_for_offset(0, &piece_table));
        assert_eq!((3, 4), piece_idx_for_offset(19, &piece_table));
        assert_eq!((0, 3), piece_idx_for_offset(3, &piece_table));
        assert_eq!((1, 2), piece_idx_for_offset(7, &piece_table));
        assert_eq!((2, 2), piece_idx_for_offset(12, &piece_table));
        assert_eq!((3, 0), piece_idx_for_offset(15, &piece_table));

        // Read 7 bytes starting at offset 6
        let offset = 6;
        let size = 7;

        let output = lookup_pieces(offset, size, &piece_table);
        assert_eq!(2, output.len());
        assert_eq!(
            PieceSlice {
                index: 1,
                begin: 1,
                end: 5,
            },
            output[0]
        );
        assert_eq!(
            PieceSlice {
                index: 2,
                begin: 0,
                end: 3,
            },
            output[1]
        );

        // Read 11 bytes starting at offset 2
        let offset = 2;
        let size = 11;

        let output = lookup_pieces(offset, size, &piece_table);
        assert_eq!(3, output.len());
        assert_eq!(
            PieceSlice {
                index: 0,
                begin: 2,
                end: 5,
            },
            output[0]
        );
        assert_eq!(
            PieceSlice {
                index: 1,
                begin: 0,
                end: 5,
            },
            output[1]
        );
        assert_eq!(
            PieceSlice {
                index: 2,
                begin: 0,
                end: 3,
            },
            output[2]
        );

        // Read 3 bytes starting at offset 12
        let offset = 12;
        let size = 3;

        let output = lookup_pieces(offset, size, &piece_table);
        assert_eq!(1, output.len());
        assert_eq!(
            PieceSlice {
                index: 2,
                begin: 2,
                end: 5,
            },
            output[0]
        );

        // Read 100 bytes starting at offset 18 (should read to the end)
        let offset = 18;
        let size = 100;

        let output = lookup_pieces(offset, size, &piece_table);
        assert_eq!(1, output.len());
        assert_eq!(
            PieceSlice {
                index: 3,
                begin: 3,
                end: 5,
            },
            output[0]
        );
    }
}
