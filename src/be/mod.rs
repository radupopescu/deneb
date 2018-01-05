//! Back-end modules of the application
//!
//! The back-end includes storage, data and metadata management etc.

use failure::ResultExt;

use std::fs::{read_dir, File};
use std::io::BufReader;
use std::path::Path;

use self::cas::read_chunks;
use self::catalog::Catalog;
use self::inode::ChunkDescriptor;
use self::store::Store;
use common::errors::{DenebError, DenebResult};

pub mod cas;
pub mod catalog;
pub mod engine;
pub mod inode;
pub mod manifest;
pub mod store;

pub fn populate_with_dir<C, S>(
    catalog: &mut C,
    store: &mut S,
    dir: &Path,
    chunk_size: usize,
) -> DenebResult<()>
where
    C: Catalog,
    S: Store,
{
    catalog.add_inode(dir, 1, vec![])?;

    let mut buffer = vec![0 as u8; chunk_size as usize];
    visit_dirs(catalog, store, buffer.as_mut_slice(), dir, 1, 1)?;

    Ok(())
}

fn visit_dirs<C, S>(
    catalog: &mut C,
    store: &mut S,
    buffer: &mut [u8],
    dir: &Path,
    dir_index: u64,
    parent_index: u64,
) -> DenebResult<()>
where
    C: Catalog,
    S: Store,
{
    catalog.add_dir_entry(dir_index, Path::new("."), dir_index)?;
    catalog.add_dir_entry(dir_index, Path::new(".."), parent_index)?;

    for entry in read_dir(dir)? {
        let path = (entry?).path();
        let fname = Path::new(path.as_path()
            .file_name()
            .ok_or_else(|| DenebError::InvalidPath(path.clone()))?);

        let mut chunks = Vec::new();
        if path.is_file() {
            let mut abs_path = dir.to_path_buf();
            abs_path.push(fname);
            let f = File::open(abs_path)?;
            let mut reader = BufReader::new(f);
            for (ref digest, ref data) in read_chunks(&mut reader, buffer)? {
                store.put_chunk(digest.clone(), data.as_ref())?;
                chunks.push(ChunkDescriptor {
                    digest: digest.clone(),
                    size: data.len(),
                });
            }
        }

        let index = catalog.get_next_index();
        catalog.add_inode(&path, index, chunks)?;
        catalog.add_dir_entry(dir_index, fname, index)?;

        if path.is_dir() {
            visit_dirs(catalog, store, buffer, &path, index, dir_index)
                .context(DenebError::DirectoryVisit(dir.to_path_buf()))?;
        }
    }
    Ok(())
}
