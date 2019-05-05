use {
    crate::{
        cas::{hash, Digest},
        errors::DenebResult,
        util::atomic_write,
    },
    log::trace,
    std::{
        fs::{copy as file_copy, create_dir_all},
        path::{Path, PathBuf},
    },
};

/// This module contains the functionality related to packing and unpacking
/// chunks in the the object store.
///
/// Packed chunks are typically stored in the "data" subdir of the store, while
/// the unpacked chunks are stored in the "scratch". An unpacked chunk is
/// created in response to a call to Store::chunk and only lives while there are
/// active references to the chunk - the backing file of the unpacked chunk is
/// deleted when the chunk is no longer referenced.
///
/// A call to Store::put_file or Store::put_file_chunked will create a packed
/// chunk in the data area of the store. The original data is hashed, compressed
/// and encrypted in the packed chunk.
///
/// The process to unpack the chunk involves saving a decrypted and decompressed
/// copy of the chunk data into the "scratch" area of the store.

const PREFIX_SIZE: usize = 2;

pub(in super) fn pack_chunk(contents: &[u8], data_root: &Path) -> DenebResult<Digest> {
    let digest = hash(contents);
    let (path_suffix, directory) = digest_to_path(&digest);
    let full_path = data_root.join(path_suffix);
    create_dir_all(data_root.join(directory))?;
    atomic_write(full_path.as_path(), contents)?;
    trace!("Chunk written: {:?}", full_path);
    Ok(digest)
}

pub(in super) fn unpack_chunk(
    digest: &Digest,
    data_root: &Path,
    scratch_root: &Path,
) -> DenebResult<PathBuf> {
    let (path_suffix, dir) = digest_to_path(digest);
    let unpacked = scratch_root.join(&path_suffix);
    create_dir_all(scratch_root.join(dir))?;
    file_copy(data_root.join(&path_suffix), &unpacked)?;
    Ok(unpacked)
}

/// Given a Digest, returns the absolute file path and the directory path
/// corresponding to the object in the store
fn digest_to_path(digest: &Digest) -> (PathBuf, PathBuf) {
    let mut prefix1 = digest.to_string();
    let mut prefix2 = prefix1.split_off(PREFIX_SIZE);
    let file_name = prefix2.split_off(PREFIX_SIZE);
    let directory = PathBuf::from(prefix1).join(prefix2);
    let file_path = directory.join(file_name);
    (file_path, directory)
}
