use {
    crate::{
        catalog::{Catalog, IndexGenerator},
        errors::{DenebError, DenebResult},
        inode::{FileAttributes, INode},
        store::Store,
    },
    failure::{Fail, ResultExt},
    nix::sys::stat::lstat,
    std::{
        fs::{read_dir, File},
        path::Path,
    },
};

pub mod cas;
pub mod catalog;
pub mod engine;
pub mod errors;
pub mod inode;
pub mod manifest;
pub mod store;
pub mod util;

mod workspace;

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
    catalog: &mut dyn Catalog,
    store: &mut dyn Store,
    dir: &Path,
    chunk_size: usize,
) -> DenebResult<()> {
    let attrs = FileAttributes::with_stats(lstat(dir)?, 1);
    catalog.add_inode(INode::new(attrs, vec![]))?;

    let mut buffer = vec![0 as u8; chunk_size as usize];
    let mut index_generator = IndexGenerator::starting_at(catalog.get_max_index());
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
    catalog: &mut dyn Catalog,
    store: &mut dyn Store,
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
        let fname = Path::new(
            path.as_path()
                .file_name()
                .ok_or_else(|| DenebError::InvalidPath(path.clone()))?,
        );

        let descriptors = if path.is_file() {
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
            )
            .context(DenebError::DirectoryVisit(dir.to_path_buf()))?;
        }
    }
    Ok(())
}
