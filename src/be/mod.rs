//! Back-end modules of the application
//!
//! The back-end includes storage, data and metadata management etc.

use std::fs::{File, read_dir};
use std::io::BufReader;
use std::path::Path;

use self::cas::read_chunks;
use self::catalog::Catalog;
use self::inode::ChunkDescriptor;
use self::store::Store;
use common::errors::*;

pub mod cas;
pub mod catalog;
pub mod engine;
pub mod inode;
pub mod manifest;
pub mod store;

pub fn populate_with_dir<C, S>(catalog: &mut C,
                               store: &mut S,
                               dir: &Path,
                               chunk_size: usize)
                               -> Result<()>
    where C: Catalog,
          S: Store
{
    catalog
        .add_inode(dir, 1, vec![])
        .chain_err(|| ErrorKind::DirVisitError(dir.to_path_buf()))?;

    let mut buffer = vec![0 as u8; chunk_size as usize];
    visit_dirs(catalog, store, buffer.as_mut_slice(), dir, 1, 1)
        .chain_err(|| ErrorKind::DirVisitError(dir.to_path_buf()))?;

    Ok(())
}

fn visit_dirs<C, S>(catalog: &mut C,
                    store: &mut S,
                    buffer: &mut [u8],
                    dir: &Path,
                    dir_index: u64,
                    parent_index: u64)
                    -> Result<()>
    where C: Catalog,
          S: Store
{
    catalog
        .add_dir_entry(dir_index, Path::new("."), dir_index)?;
    catalog
        .add_dir_entry(dir_index, Path::new(".."), parent_index)?;

    for entry in read_dir(dir)? {
        let path = (entry?).path();
        let fpath = &path.as_path();
        let fname = Path::new(fpath
                                  .file_name()
                                  .ok_or_else(|| "Could not get file name from path")?);

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
        catalog.add_inode(fpath, index, chunks)?;
        catalog.add_dir_entry(dir_index, fname, index)?;

        if path.is_dir() {
            visit_dirs(catalog, store, buffer, &path, index, dir_index)?;
        }
    }
    Ok(())
}
