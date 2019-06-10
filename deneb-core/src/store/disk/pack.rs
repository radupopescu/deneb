use {
    crate::{
        cas::{hash, Digest},
        crypt::{decrypt, encrypt, EncryptionKey, Nonce},
        errors::DenebResult,
        util::create_temp_file,
    },
    failure::{Fail, ResultExt},
    log::trace,
    scopeguard::defer,
    serde::{Deserialize, Serialize},
    std::{
        cell::Cell,
        fs::{create_dir_all, remove_file, rename, File},
        io::{Read, Write},
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

#[derive(Debug, Fail)]
#[fail(display = "Missing encryption key")]
pub struct MissingKeyError;

#[derive(Debug, Fail)]
#[fail(display = "Chunk body I/O error")]
pub struct ChunkIOError;

/// The header is written at the beginning of each packed chunk file. It
/// contains the packing parameters for the chunk:
/// - whether compression was used
/// - nonce used for encryption
#[derive(Deserialize, Serialize)]
struct Header {
    compressed: bool,
    nonce: Option<Nonce>,
}

pub(super) fn pack_chunk(
    contents: &[u8],
    packed_root: &Path,
    compressed: bool,
    encryption_key: Option<&EncryptionKey>,
) -> DenebResult<Digest> {
    let digest = hash(contents);
    let (path_suffix, directory) = digest_to_path(&digest);
    let full_path = packed_root.join(path_suffix);
    // ensure all needed dirs are created in the data dir
    create_dir_all(packed_root.join(directory))?;

    // Create the temporary file and set up an RAII guard to delete it
    // in case of errors
    let cleanup = Cell::new(true);
    let (mut f, temp_path) = create_temp_file(&full_path)?;
    defer! {{
        if cleanup.get() {
            remove_file(&temp_path).expect("could not delete temporary file");
        }
    }}

    // Optionally encrypt the body of the chunk
    let (contents, nonce) = if let Some(key) = encryption_key {
        let nonce = Nonce::new();
        let ciphertext = encrypt(contents, &nonce, key);
        (ciphertext, Some(nonce))
    } else {
        (contents.to_owned(), None)
    };

    // the header contains the packing parameters (compression, encryption
    // nonce)
    let header = Header { compressed, nonce };

    // the header is written without compression or encryption
    let header = bincode::serialize(&header)?;
    std::io::copy(&mut header.as_slice(), &mut f).context("could not write chunk header")?;

    if compressed {
        copy_body(&mut contents.as_slice(), &mut snap::Writer::new(f))?;
    } else {
        copy_body(&mut contents.as_slice(), &mut f)?;
    }

    rename(&temp_path, &full_path)?;

    // Packing was successful. Disable RAII cleanup guard
    cleanup.set(false);

    trace!("Chunk written: {:?}", full_path);
    Ok(digest)
}

pub(super) fn unpack_chunk(
    digest: &Digest,
    packed_root: &Path,
    unpacked_root: &Path,
    encryption_key: Option<&EncryptionKey>,
) -> DenebResult<PathBuf> {
    let (path_suffix, dir) = digest_to_path(digest);
    let unpacked_file_name = unpacked_root.join(&path_suffix);
    create_dir_all(unpacked_root.join(dir))?;

    let mut packed = File::open(packed_root.join(&path_suffix))?;

    let header = bincode::deserialize_from::<_, Header>(Read::by_ref(&mut packed))?;

    let mut buffer = Vec::new();
    if header.compressed {
        copy_body(&mut snap::Reader::new(packed), &mut buffer)?;
    } else {
        copy_body(&mut packed, &mut buffer)?;
    }

    let body = if let Some(nonce) = header.nonce {
        let key = encryption_key.ok_or(MissingKeyError)?;
        decrypt(&buffer, &nonce, &key)
    } else {
        Ok(buffer)
    }?;

    // Create the temporary file and set up an RAII guard to delete it
    // in case of errors
    let cleanup = Cell::new(true);
    let (mut unpacked, temp_path) = create_temp_file(&unpacked_file_name)?;
    defer! {{
        if cleanup.get() {
            remove_file(&temp_path).expect("could not delete temporary file");
        }
    }}

    copy_body(&mut body.as_slice(), &mut unpacked)?;

    rename(&temp_path, &unpacked_file_name)?;

    // Packing was successful. Disable RAII cleanup guard
    cleanup.set(false);

    Ok(unpacked_file_name)
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

fn copy_body(src: &mut impl Read, dst: &mut impl Write) -> DenebResult<()> {
    std::io::copy(src, dst).context(ChunkIOError)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::cas::hash,
        rand::{thread_rng, RngCore},
        tempdir::TempDir,
    };

    const TEST_CHUNK_SIZE: usize = 1024 * 1024; // 1 MB

    #[test]
    fn pack_unpack_uncompressed() -> DenebResult<()> {
        let tmp = TempDir::new("chunk_packing_uncompressed")?;
        let packed_root = tmp.path().join("packed");
        let unpacked_root = tmp.path().join("unpacked");
        create_dir_all(&packed_root)?;
        create_dir_all(&unpacked_root)?;

        let mut data = vec![0 as u8; TEST_CHUNK_SIZE];
        thread_rng().fill_bytes(data.as_mut());

        let digest_in = pack_chunk(&data, &packed_root, false, None)?;
        let unpacked = unpack_chunk(&digest_in, &packed_root, &unpacked_root, None)?;

        let mut f = File::open(unpacked)?;
        let mut read_back = vec![];
        f.read_to_end(read_back.as_mut())?;

        let digest_out = hash(&read_back);

        assert_eq!(digest_in, digest_out);

        Ok(())
    }

    #[test]
    fn pack_unpack_uncompressed_encrypted() -> DenebResult<()> {
        let tmp = TempDir::new("chunk_packing_uncompressed_encrypted")?;
        let packed_root = tmp.path().join("packed");
        let unpacked_root = tmp.path().join("unpacked");
        create_dir_all(&packed_root)?;
        create_dir_all(&unpacked_root)?;

        let mut data = vec![0 as u8; TEST_CHUNK_SIZE];
        thread_rng().fill_bytes(data.as_mut());

        let key = Some(EncryptionKey::new());

        let digest_in = pack_chunk(&data, &packed_root, false, key.as_ref())?;
        let unpacked = unpack_chunk(&digest_in, &packed_root, &unpacked_root, key.as_ref())?;

        let mut f = File::open(unpacked)?;
        let mut read_back = vec![];
        f.read_to_end(read_back.as_mut())?;

        let digest_out = hash(&read_back);

        assert_eq!(digest_in, digest_out);

        Ok(())
    }

    #[test]
    fn pack_unpack_compressed() -> DenebResult<()> {
        let tmp = TempDir::new("chunk_packing_compressed")?;
        let packed_root = tmp.path().join("packed");
        let unpacked_root = tmp.path().join("unpacked");
        create_dir_all(&packed_root)?;
        create_dir_all(&unpacked_root)?;

        let mut data = vec![0 as u8; TEST_CHUNK_SIZE];
        thread_rng().fill_bytes(data.as_mut());

        let digest_in = pack_chunk(&data, &packed_root, true, None)?;
        let unpacked = unpack_chunk(&digest_in, &packed_root, &unpacked_root, None)?;

        let mut f = File::open(unpacked)?;
        let mut read_back = vec![];
        f.read_to_end(read_back.as_mut())?;

        let digest_out = hash(&read_back);

        assert_eq!(digest_in, digest_out);

        Ok(())
    }

    #[test]
    fn pack_unpack_compressed_encrypted() -> DenebResult<()> {
        let tmp = TempDir::new("chunk_packing_compressed_encrypted")?;
        let packed_root = tmp.path().join("packed");
        let unpacked_root = tmp.path().join("unpacked");
        create_dir_all(&packed_root)?;
        create_dir_all(&unpacked_root)?;

        let mut data = vec![0 as u8; TEST_CHUNK_SIZE];
        thread_rng().fill_bytes(data.as_mut());

        let key = Some(EncryptionKey::new());

        let digest_in = pack_chunk(&data, &packed_root, true, key.as_ref())?;
        let unpacked = unpack_chunk(&digest_in, &packed_root, &unpacked_root, key.as_ref())?;

        let mut f = File::open(unpacked)?;
        let mut read_back = vec![];
        f.read_to_end(read_back.as_mut())?;

        let digest_out = hash(&read_back);

        assert_eq!(digest_in, digest_out);

        Ok(())
    }
}
