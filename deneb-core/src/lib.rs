extern crate bincode;
extern crate data_encoding;
#[macro_use]
extern crate failure;
extern crate lmdb;
extern crate lmdb_sys;
#[macro_use]
extern crate log;
extern crate lru;
extern crate memmap;
extern crate nix;
extern crate rust_sodium;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate time;
extern crate toml;

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate tempdir;

use failure::ResultExt;

use nix::sys::stat::lstat;

use std::fs::{read_dir, File};
use std::path::Path;

use self::catalog::{Catalog, IndexGenerator};
use self::errors::{DenebError, DenebResult};
use self::inode::{FileAttributes, INode};
use self::store::Store;

pub mod cas;
pub mod catalog;
pub mod engine;
pub mod errors;
pub mod inode;
pub mod manifest;
pub mod store;
pub mod util;

mod dir_workspace;
mod file_workspace;

#[derive(Debug, Fail)]
pub enum DenebCoreInitError {
    #[fail(display = "Could not initialize the rust_sodium library")]
    RustSodium,
}

pub fn init() -> DenebResult<()> {
    // Initialize the rust_sodium library (needed to make all its functions thread-safe)
    rust_sodium::init().map_err(|_| DenebCoreInitError::RustSodium)?;

    Ok(())
}

pub fn populate_with_dir(
    catalog: &mut Box<dyn Catalog>,
    store: &mut Box<dyn Store>,
    dir: &Path,
    chunk_size: usize,
) -> DenebResult<()> {
    let attrs = FileAttributes::with_stats(lstat(dir)?, 1);
    catalog.add_inode(INode::new(attrs, vec![]))?;

    let mut buffer = vec![0 as u8; chunk_size as usize];
    let mut index_generator = IndexGenerator::starting_at(catalog.get_max_index())?;
    visit_dirs(
        catalog,
        store,
        &mut index_generator,
        buffer.as_mut_slice(),
        dir,
        1,
        1,
    )?;

    Ok(())
}

fn visit_dirs(
    catalog: &mut Box<dyn Catalog>,
    store: &mut Box<dyn Store>,
    index_generator: &mut IndexGenerator,
    buffer: &mut [u8],
    dir: &Path,
    dir_index: u64,
    parent_index: u64,
) -> DenebResult<()> {
    catalog.add_dir_entry(dir_index, Path::new("."), dir_index)?;
    catalog.add_dir_entry(dir_index, Path::new(".."), parent_index)?;

    for entry in read_dir(dir)? {
        let path = (entry?).path();
        let fname = Path::new(path.as_path()
            .file_name()
            .ok_or_else(|| DenebError::InvalidPath(path.clone()))?);

        let mut descriptors = if path.is_file() {
            let mut abs_path = dir.to_path_buf();
            abs_path.push(fname);
            let mut f = File::open(abs_path)?;
            store.put_file_chunked(&mut f)?
        } else {
            Vec::new()
        };

        let index = index_generator.get_next();
        let attrs = FileAttributes::with_stats(lstat(&path)?, index);
        catalog.add_inode(INode::new(attrs, descriptors))?;
        catalog.add_dir_entry(dir_index, fname, index)?;

        if path.is_dir() {
            visit_dirs(
                catalog,
                store,
                index_generator,
                buffer,
                &path,
                index,
                dir_index,
            ).context(DenebError::DirectoryVisit(dir.to_path_buf()))?;
        }
    }
    Ok(())
}
