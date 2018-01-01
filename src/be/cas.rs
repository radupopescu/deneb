use data_encoding::HEXLOWER;
use rust_sodium::crypto::hash::sha512::Digest as SodiumDigest;
use rust_sodium::crypto::hash::hash as sodium_hash;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, Visitor};

use std::fmt;
use std::io::BufRead;

use common::errors::*;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Digest(SodiumDigest);

impl Digest {
    fn new(digest: SodiumDigest) -> Digest {
        Digest(digest)
    }

    pub fn to_string(&self) -> String {
        let &Digest(SodiumDigest(digest)) = self;
        HEXLOWER.encode(&digest)
    }

    pub fn from_slice(s: &[u8]) -> Result<Digest> {
        let decoded = HEXLOWER.decode(s)?;
        if let Some(sd) = SodiumDigest::from_slice(decoded.as_slice()) {
            Ok(Digest(sd))
        } else {
            bail!("Could not decode string as HEXLOWER")
        }
    }
}


impl Serialize for Digest {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
        where S: Serializer
    {
        let s = self.to_string();
        serializer.serialize_str(s.as_str())
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D>(deserializer: D) -> ::std::result::Result<Digest, D::Error>
        where D: Deserializer<'de>
    {
        struct DigestVisitor;

        impl<'de> Visitor<'de> for DigestVisitor {
            type Value = Digest;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("A string representing a HEXLOWER encoding of a SHA512 digest")
            }

            fn visit_str<E>(self, v: &str) -> ::std::result::Result<Self::Value, E>
                where E: Error
            {
                Digest::from_slice(v.as_bytes()).map_err(Error::custom)
            }
        }

        deserializer.deserialize_str(DigestVisitor)
    }
}

fn hash_buf(buffer: &[u8]) -> (Digest, Vec<u8>) {
    let digest = hash(buffer);
    (digest, buffer.to_vec())
}

pub fn hash(msg: &[u8]) -> Digest {
    Digest::new(sodium_hash(msg))
}

pub fn read_chunks<R: BufRead>(mut reader: R, buffer: &mut [u8]) -> Result<Vec<(Digest,Vec<u8>)>> {
    let chunk_size = buffer.len();
    let mut chunks = Vec::new();
    let mut offset = 0;
    loop {
        match reader.read(&mut buffer[offset..]) {
            Ok(n) => {
                if n > 0 {
                    offset += n;
                    if offset == chunk_size {
                        chunks.push(hash_buf(&buffer[0..offset]));
                        offset = 0;
                    }
                } else if n == 0 {
                    break;
                }
            }
            Err(e) => {
                if e.kind() == ::std::io::ErrorKind::Interrupted {
                    // Retry if interrupted
                    continue;
                } else {
                    bail!(ErrorKind::IoError(e));
                }
            }
        }
    }

    if offset > 0 {
        chunks.push(hash_buf(&buffer[0..offset]));
    }

    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use quickcheck::{QuickCheck, StdGen, TestResult};
    use rand::{Rng, thread_rng};

    use super::*;

    #[test]
    fn digest_to_string_and_back() {
        let digest = hash("some_key".as_ref());
        let serialized = digest.to_string();
        assert_eq!(serialized, "41bcc5cb17c49e80e1f20fde666dedad51bc35f146051da2689419948c07a4974e65be08e41fc194126a3e162aee9165271a32119e0cd369e587cf519a68e293");

        let deserialized = Digest::from_slice(serialized.as_bytes());
        assert!(deserialized.is_ok());
        if let Ok(deserialized) = deserialized {
            assert_eq!(digest, deserialized);
        }
    }

    fn helper(file_size: usize, chunk_size: u64) -> Result<bool> {
        let mut contents = vec![0 as u8; file_size];
        thread_rng().fill_bytes(contents.as_mut());
        let mut buffer = vec![0 as u8; chunk_size as usize];
        let chunks = read_chunks(contents.as_slice(), buffer.as_mut_slice())?;

        let mut combined_chunks = Vec::new();
        for &(_, ref data) in &chunks {
            combined_chunks.append(&mut data.clone());
        }

        let enough_chunks = chunks.len() >= ((file_size as u64) / chunk_size) as usize;
        let correct_size = file_size == combined_chunks.len();
        let correct_data = contents == combined_chunks;

        Ok(enough_chunks && correct_size && correct_data)
    }

    #[test]
    fn digest_small_file_gives_single_chunk() {
        let res = helper(5, 10);
        assert!(res.is_ok());
        if let Ok(res) = res {
            assert!(res);
        }
    }

    #[test]
    fn digest_prop_large_files_are_chunked() {
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
            .tests(100)
            .gen(StdGen::new(thread_rng(), 100))
            .quickcheck(large_files_are_chunked as fn((usize, u64)) -> TestResult);
    }
}