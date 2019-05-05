use {
    crate::errors::{DenebError, DenebResult},
    data_encoding::HEXLOWER,
    rust_sodium::crypto::hash::{hash as sodium_hash, sha512::Digest as SodiumDigest},
    serde::{
        de::{Error, Visitor},
        Deserialize, Deserializer, Serialize, Serializer,
    },
    std::fmt::{Display, Formatter, Result as FmtResult},
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Digest(SodiumDigest);

pub fn hash(msg: &[u8]) -> Digest {
    Digest(sodium_hash(msg))
}

/// Reads bytes from an input source and produces a series of chunks
///
/// Reads bytes from an input source into a buffer. Each time the buffer
/// is full, or when there are no more bytes to be read from the input,
/// the given processing function will be called to operate on the contents
/// of the buffer
pub(crate) fn read_chunked<R: ::std::io::Read>(
    mut reader: R,
    buffer: &mut [u8],
    mut processor: impl FnMut(&[u8]) -> DenebResult<()>,
) -> DenebResult<()> {
    let chunk_size = buffer.len();
    let mut offset = 0;
    loop {
        match reader.read(&mut buffer[offset..]) {
            Ok(n) => {
                if n > 0 {
                    offset += n;
                    if offset == chunk_size {
                        processor(&buffer[0..offset])?;
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
                    return Err(DenebError::DiskIO.into());
                }
            }
        }
    }

    if offset > 0 {
        processor(&buffer[0..offset])?;
    }

    Ok(())
}

impl Display for Digest {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let &Digest(SodiumDigest(digest)) = self;
        write!(f, "{}", HEXLOWER.encode(&digest))
    }
}

impl Serialize for Digest {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = self.to_string();
        serializer.serialize_str(s.as_str())
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D>(deserializer: D) -> ::std::result::Result<Digest, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DigestVisitor;

        impl<'de> Visitor<'de> for DigestVisitor {
            type Value = Digest;

            fn expecting(&self, formatter: &mut Formatter) -> FmtResult {
                formatter.write_str("A string representing a HEXLOWER encoding of a SHA512 digest")
            }

            fn visit_str<E>(self, v: &str) -> ::std::result::Result<Self::Value, E>
            where
                E: Error,
            {
                digest_from_slice(v.as_bytes()).map_err(Error::custom)
            }
        }

        deserializer.deserialize_str(DigestVisitor)
    }
}

fn digest_from_slice(s: &[u8]) -> DenebResult<Digest> {
    let decoded = HEXLOWER.decode(s)?;
    if let Some(sd) = SodiumDigest::from_slice(decoded.as_slice()) {
        Ok(Digest(sd))
    } else {
        Err(DenebError::DigestFromSlice.into())
    }
}

#[cfg(test)]
mod tests {
    use quickcheck::{QuickCheck, RngCore, StdGen, TestResult};
    use rand::thread_rng;

    use super::*;

    #[test]
    fn digest_to_string_and_back() -> DenebResult<()> {
        let digest = hash("some_key".as_ref());
        let serialized = digest.to_string();
        #[rustfmt::skip]
        assert_eq!(serialized, "41bcc5cb17c49e80e1f20fde666dedad51bc35f146051da2689419948c07a4974e65be08e41fc194126a3e162aee9165271a32119e0cd369e587cf519a68e293");

        let deserialized = digest_from_slice(serialized.as_bytes())?;
        assert_eq!(digest, deserialized);
        Ok(())
    }

    fn helper(file_size: usize, chunk_size: u64) -> DenebResult<bool> {
        let mut contents = vec![0 as u8; file_size];
        thread_rng().fill_bytes(contents.as_mut());
        let mut buffer = vec![0 as u8; chunk_size as usize];
        let mut chunks = vec![];
        read_chunked(contents.as_slice(), buffer.as_mut_slice(), |s| {
            let digest = hash(s);
            chunks.push((digest, s.to_vec()));
            Ok(())
        })?;

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
    fn digest_small_file_gives_single_chunk() -> DenebResult<()> {
        let res = helper(5, 10)?;
        assert!(res);
        Ok(())
    }

    #[test]
    fn digest_prop_large_files_are_chunked() {
        fn large_files_are_chunked(pair: (usize, u64)) -> TestResult {
            let (file_size, chunk_size) = pair;
            if chunk_size == 0 {
                TestResult::discard()
            } else {
                TestResult::from_bool(if let Ok(res) = helper(file_size, chunk_size) {
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
