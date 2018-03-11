use std::cell::RefCell;
use std::cmp::min;
use std::rc::Rc;
use std::sync::Arc;

use cas::Digest;
use errors::DenebResult;
use inode::{ChunkDescriptor, FileAttributes, INode};
use store::Store;

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
pub(crate) struct FileWorkspace<S> {
    #[allow(dead_code)] attributes: FileAttributes,
    lower: Lower<S>,
    upper: Vec<u8>,
    piece_table: Vec<Piece>,
}

impl<S> FileWorkspace<S>
where
    S: Store,
{
    /// Create a new `FileWorkspace` for an `INode`
    ///
    /// Constructs a new workspace object for the file described by
    /// `inode`. The function takes a reference-counted pointer to a
    /// `Store` object which is used by the underlying `Chunks` making
    /// up the lower, immutable, layer
    pub(crate) fn new(inode: &INode, store: Rc<RefCell<S>>) -> FileWorkspace<S> {
        let lower = Lower::new(inode.chunks.as_slice(), store);
        let piece_table = lower
            .chunks
            .iter()
            .enumerate()
            .map(|(idx, ref c)| {
                let chunk_size = c.borrow().size;
                Piece {
                    target: PieceTarget::Lower(idx),
                    offset: 0,
                    size: chunk_size,
                }
            })
            .collect::<Vec<_>>();
        FileWorkspace {
            attributes: inode.attributes,
            lower,
            upper: vec![],
            piece_table,
        }
    }

    /// Read `size` number of bytes, starting at `offset`
    pub(crate) fn read_at(&self, offset: usize, size: usize) -> DenebResult<Vec<u8>> {
        let slices = lookup_pieces(offset, size, &self.piece_table);
        let buffer = self.fill_buffer(&slices)?;
        Ok(buffer)
    }

    /// Write the contents of buffer into the workspace, starting at `offset`
    pub(crate) fn write_at(&mut self, offset: usize, buffer: &[u8]) -> DenebResult<()> {
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
        if self.piece_table.is_empty() || (offset as u64 >= self.attributes.size) {
            if offset as u64 > self.attributes.size {
                self.piece_table.push(Piece {
                    target: PieceTarget::Zero,
                    offset: 0,
                    size: offset - self.attributes.size as usize,
                });
            }
            self.piece_table.push(new_piece);
            self.attributes.size = (offset + buf_size) as u64;
            return Ok(());
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
        if (offset + buf_size) as u64 >= self.attributes.size {
            self.attributes.size = (offset + buf_size) as u64;
            self.piece_table = new_piece_table;
            return Ok(());
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

        Ok(())
    }

    /// Unload the lower layer from memory
    ///
    /// Forces the lower layer of the workspace to be unloaded from
    /// memory, when "closing" the workspace is desired, while
    /// maintaining any changes recorded in the top layer.
    pub(crate) fn unload(&self) {
        self.lower.unload();
    }

    fn fill_buffer(&self, slices: &[PieceSlice]) -> DenebResult<Vec<u8>> {
        let mut buffer = vec![];
        for &PieceSlice { index, begin, end } in slices {
            let piece = &self.piece_table[index];
            match piece.target {
                PieceTarget::Lower(chunk_index) => {
                    let mut chunk = self.lower.chunks[chunk_index].borrow_mut();
                    let slice = chunk.get_slice()?;
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
/// If a piece points the lower layer, and index is provided whic identifies which
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
struct Lower<S> {
    chunks: Vec<RefCell<Chunk<S>>>,
}

impl<S> Lower<S>
where
    S: Store,
{
    /// Construct the lower layer using a provided list of `ChunkDescriptor`
    fn new(chunk_descriptors: &[ChunkDescriptor], store: Rc<RefCell<S>>) -> Lower<S> {
        let mut chunks = vec![];
        for &ChunkDescriptor { digest, size } in chunk_descriptors {
            chunks.push(RefCell::new(Chunk::new(digest, size, Rc::clone(&store))));
        }
        Lower { chunks }
    }

    /// Unload the lower layer from memory
    fn unload(&self) {
        for c in self.chunks.iter() {
            let mut chk = c.borrow_mut();
            chk.unload();
        }
    }
}

/// An interface to the file chunks stored in a repository
///
/// The `Chunk` type, allows reading a file chunk identified by a
/// `Digest` from a repository. This type provides a read-only view of
/// the chunk, but the mutable aspect of the type comes from the
/// caching behaviour: the byte vector returned by the object store
/// (wrapped in an `Arc`) is cached by this type. Calling `unload`
/// will release this cached vector.
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
    /// Construct a new `Chunk` of `size`, identified by `Digest`
    ///
    /// The newly constructed object maintains an pointer to a
    /// `Store`, used to retrieve the content of the chunk only when
    /// needed
    fn new(digest: Digest, size: usize, store: Rc<RefCell<S>>) -> Chunk<S> {
        Chunk {
            digest,
            size,
            store,
            data: None,
        }
    }

    /// Discard the cached content of the chunk
    ///
    /// After calling this method, when the content of the chunk is
    /// again requested, the chunk needs to be retrieved from the
    /// `Store`, potentially involving a costly decompression and
    /// decryption process
    fn unload(&mut self) {
        if self.data.is_some() {
            self.data = None;
        }
    }

    /// Return the content of the chunk in a slice
    ///
    /// This method potentially involves retrieving the content of the
    /// chunk from the `Store`. Upon retrieval, the contents are
    /// cached in memory, so subsequent calls to this method are fast
    fn get_slice(&mut self) -> DenebResult<&[u8]> {
        if self.data.is_none() {
            self.data = Some(self.store.borrow().get_chunk(&self.digest)?);
        }
        // Note: The following unwrap should never panic
        Ok(self.data.as_ref().unwrap().as_slice())
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

    use store::MemStore;
    use util::run;

    #[test]
    fn file_workspace_read() {
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
            let ws = FileWorkspace::new(&inode, Rc::clone(&store));

            let res = ws.read_at(0, 17)?;

            assert_eq!(b"alabalaportocala", res.as_slice());

            ws.unload();

            Ok(())
        });
    }

    #[test]
    fn file_workspace_write() {
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

            let res0 = ws.read_at(0, 17)?;
            assert_eq!(b"alabalaportocala", res0.as_slice());

            assert!(ws.write_at(2, b"written").is_ok());

            let res1 = ws.read_at(0, 17)?;
            assert_eq!(b"alwrittenrtocala", res1.as_slice());

            assert!(ws.write_at(6, b"again").is_ok());

            let res2 = ws.read_at(0, 17)?;
            assert_eq!(b"alwritagainocala", res2.as_slice());

            ws.unload();

            Ok(())
        });
    }

    fn make_piece_table(input_size: usize, chunk_size: usize) -> Vec<Piece> {
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
    }

    #[test]
    fn locate_slice() {
        let piece_table = make_piece_table(20, 5);

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
