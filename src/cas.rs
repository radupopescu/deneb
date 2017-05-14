use rust_sodium::crypto::hash::sha512;
use rust_sodium::crypto::hash::hash;

use std::fs::File;
use std::io::{Read, BufReader};

use errors::*;
use store::Store;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Digest(sha512::Digest);

impl Digest {
    pub fn new(digest: sha512::Digest) -> Digest {
        Digest(digest)
    }
}

pub struct Chunk {
    pub digest: Digest,
    pub data: Vec<u8>,
}

pub fn read_chunks(file: &File, chunk_size: u64) -> Result<Vec<Chunk>> {
    let mut chunks = Vec::new();
    let mut buffer = Vec::new();
    let _ = BufReader::new(file).read_to_end(&mut buffer)?;
    let digest = hash(buffer.as_ref());
    chunks.push(Chunk { digest: Digest::new(digest), data: buffer } );
    Ok(chunks)
}
