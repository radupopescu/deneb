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

pub fn hash(msg: &[u8]) -> Digest {
    Digest::new(sodium_hash(msg))
}

pub fn read_chunks(file: &File, chunk_size: u64) -> Result<Vec<Chunk>> {
    let mut chunks = Vec::new();
    let mut buffer = Vec::new();
    let _ = BufReader::new(file).read_to_end(&mut buffer)?;
    let digest = hash(buffer.as_ref());
    chunks.push(Chunk {
                    digest: digest,
                    data: buffer,
                });
    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use quickcheck::{QuickCheck, StdGen, TestResult};
    use rand::{Rng, thread_rng};
    use tempdir::TempDir;

    use std::io::Write;

    use super::*;

    fn helper(chunk_size: u64, file_size: usize) -> Result<bool> {
        let tmp_dir = TempDir::new("/tmp/deneb_chunk_test")?;
        let file_name = tmp_dir.path().join("input_file.txt");
        let mut file = File::create(&file_name)?;
        let mut contents = vec![0 as u8; file_size];
        thread_rng().fill_bytes(contents.as_mut());
        file.write_all(&contents)?;
        let file2 = File::open(&file_name)?;
        let chunks = read_chunks(&file2, chunk_size)?;
        Ok(chunks.len() >= ((file_size as u64) / chunk_size) as usize)
    }

    #[test]
    fn small_file_gives_single_chunk() {
        let res = helper(10, 5);
        assert!(res.is_ok());
        if let Ok(res) = res {
            assert!(res);
        }
    }

    #[test]
    fn prop_large_files_are_chunked() {
        fn large_files_are_chunked(pair: (u64, usize)) -> TestResult {
            let (mut chunk_size, file_size) = pair;
            if chunk_size == 0 {
                TestResult::discard()
            } else {
                let res = if let Ok(res) = helper(chunk_size, file_size) {
                    res
                } else {
                    false
                };
                TestResult::from_bool(res)
            }
        }
        QuickCheck::new()
            .tests(10)
            .gen(StdGen::new(thread_rng(), 100))
            .quickcheck(large_files_are_chunked as fn((u64, usize)) -> TestResult);
    }
}
