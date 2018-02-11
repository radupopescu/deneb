extern crate bincode;
extern crate data_encoding;
#[macro_use]
extern crate failure;
extern crate futures;
extern crate lmdb;
extern crate lmdb_sys;
#[macro_use]
extern crate log;
extern crate lru;
extern crate nix;
extern crate rust_sodium;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate time;
extern crate tokio_core;
extern crate toml;

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate tempdir;

use failure::ResultExt;

use std::fs::{read_dir, File};
use std::path::Path;

use self::catalog::Catalog;
use self::errors::{DenebError, DenebResult};
use self::store::Store;

pub mod cas;
pub mod catalog;
pub mod engine;
pub mod errors;
pub mod inode;
pub mod manifest;
pub mod store;
pub mod util;

//mod file_workspace;

#[derive(Debug, Fail)]
pub enum DenebCoreInitError {
    #[fail(display = "Could not initialize the rust_sodium library")] RustSodium,
}

pub fn init() -> Result<(), DenebCoreInitError> {
    // Initialize the rust_sodium library (needed to make all its functions thread-safe)
    if !rust_sodium::init() {
        return Err(DenebCoreInitError::RustSodium);
    }
    Ok(())
}

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

        let mut descriptors = Vec::new();
        if path.is_file() {
            let mut abs_path = dir.to_path_buf();
            abs_path.push(fname);
            let mut f = File::open(abs_path)?;
            descriptors = store.put_file_chunked(&mut f)?;
        }

        let index = catalog.get_next_index();
        catalog.add_inode(&path, index, descriptors)?;
        catalog.add_dir_entry(dir_index, fname, index)?;

        if path.is_dir() {
            visit_dirs(catalog, store, buffer, &path, index, dir_index)
                .context(DenebError::DirectoryVisit(dir.to_path_buf()))?;
        }
    }
    Ok(())
}