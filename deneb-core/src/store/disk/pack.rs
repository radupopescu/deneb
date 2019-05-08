use {
    crate::{
        cas::{hash, Digest},
        errors::DenebResult,
        util::create_temp_file,
    },
    failure::ResultExt,
    log::trace,
    scopeguard::defer,
    serde::{Deserialize, Serialize},
    std::{
        cell::Cell,
        fs::{create_dir_all, remove_file, rename},
        io::Write,
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
const MIN_COMPRESSION_THRESHOLD: usize = 1024 * 1024;

/// The header is written at the beginning of each packed chunk file. It
/// contains the packing parameters for the chunk:
/// - whether compression was used
/// - nonce used for encryption
#[derive(Serialize, Deserialize)]
struct Header {
    compressed: bool,
}

pub(super) fn pack_chunk(contents: &[u8], data_root: &Path) -> DenebResult<Digest> {
    let digest = hash(contents);
    let (path_suffix, directory) = digest_to_path(&digest);
    let full_path = data_root.join(path_suffix);
    // ensure all needed dirs are created in the data dir
    create_dir_all(data_root.join(directory))?;

    // the header contains the packing parameters
    let mut header = Header { compressed: false };

    if contents.len() >= MIN_COMPRESSION_THRESHOLD {
        header.compressed = true;
    }

    // Create the temporary file and set up an RAII guard to delete it
    // in case of errors
    let cleanup = Cell::new(true);
    let (mut f, temp_path) = create_temp_file(&full_path)?;
    defer! {{
        if cleanup.get() {
            remove_file(&temp_path).expect("could not delete temporary file");
        }
    }}

    // the header is written without compression or encryption
    let hd = bincode::serialize(&header)?;
    std::io::copy(&mut hd.as_slice(), &mut f).context("could not write chunk header")?;

    if header.compressed {
        write_body(&contents, snap::Writer::new(f))?;
    } else {
        write_body(&contents, f)?;
    }

    rename(&temp_path, &full_path)?;

    // Packing was successful. Disable RAII cleanup guard
    cleanup.set(false);

    trace!("Chunk written: {:?}", full_path);
    Ok(digest)
}

pub(super) fn unpack_chunk(
    digest: &Digest,
    data_root: &Path,
    scratch_root: &Path,
) -> DenebResult<PathBuf> {
    let (path_suffix, dir) = digest_to_path(digest);
    let unpacked = scratch_root.join(&path_suffix);
    create_dir_all(scratch_root.join(dir))?;
    std::fs::copy(data_root.join(&path_suffix), &unpacked)?;
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

fn write_body(mut contents: &[u8], mut w: impl Write) -> DenebResult<()> {
    std::io::copy(&mut (contents), &mut w).context("could not write chunk body")?;
    Ok(())
}
