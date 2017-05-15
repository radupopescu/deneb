use rust_sodium::crypto::hash::sha512;
use rust_sodium::crypto::hash::hash as sodium_hash;

use std::fs::File;
use std::io::{Read, BufReader};

use common::errors::*;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Digest(sha512::Digest);

impl Digest {
    fn new(digest: sha512::Digest) -> Digest {
        Digest(digest)
    }
}

pub struct Chunk {
    pub digest: Digest,
    pub data: Vec<u8>,
}

impl Chunk {
    fn from_buf(buffer: &[u8]) -> Chunk {
        let digest = hash(buffer);
        Chunk { digest: digest, data: buffer.to_vec() }
    }
}

pub fn hash(msg: &[u8]) -> Digest {
    Digest::new(sodium_hash(msg))
}

pub fn read_chunks(file: &File, chunk_size: u64) -> Result<Vec<Chunk>> {
    let mut chunks = Vec::new();
    let mut buffer = vec![0 as u8; chunk_size as usize];
    let mut offset = 0;
    let mut reader = BufReader::new(file);
    loop {
        let n = reader.read(&mut buffer[offset..])?;
        if n > 0 {
            offset += n;
            if offset as u64 == chunk_size {
                chunks.push(Chunk::from_buf(&buffer[0..offset]));
                offset = 0;
            }
        } else {
            break;
        }
    }

    if offset > 0 {
        chunks.push(Chunk::from_buf(&buffer[0..offset]));
    }

    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use quickcheck::{QuickCheck, StdGen, TestResult};
    use rand::{Rng, thread_rng};
    use tempdir::TempDir;

    use std::io::Write;

    use super::*;

    fn helper(file_size: usize, chunk_size: u64) -> Result<bool> {
        let tmp_dir = TempDir::new("/tmp/deneb_chunk_test")?;
        let file_name = tmp_dir.path().join("input_file.txt");
        let mut file = File::create(&file_name)?;
        let mut contents = vec![0 as u8; file_size];
        thread_rng().fill_bytes(contents.as_mut());
        file.write_all(&contents)?;
        let file2 = File::open(&file_name)?;
        let chunks = read_chunks(&file2, chunk_size)?;

        let mut combined_chunks = Vec::new();
        for chunk in &chunks {
            combined_chunks.append(&mut chunk.data.clone());
        }

        let enough_chunks = chunks.len() >= ((file_size as u64) / chunk_size) as usize;
        let correct_size = file_size == combined_chunks.len();
        let correct_data = contents == combined_chunks;

        Ok(enough_chunks && correct_size && correct_data)
    }

    #[test]
    fn small_file_gives_single_chunk() {
        let res = helper(5, 10);
        assert!(res.is_ok());
        if let Ok(res) = res {
            assert!(res);
        }
    }

    #[test]
    fn prop_large_files_are_chunked() {
        fn large_files_are_chunked(pair: (usize, u64)) -> TestResult {
            let (file_size, chunk_size) = pair;
            if chunk_size == 0 {
                TestResult::discard()
            } else {
                TestResult::from_bool(
                    if let Ok(res) = helper(file_size, chunk_size) {
                        res
                    } else {
                        false
                    })
            }
        }
        QuickCheck::new()
            .tests(50)
            .gen(StdGen::new(thread_rng(), 100))
            .quickcheck(large_files_are_chunked as fn((usize, u64)) -> TestResult);
    }
}
