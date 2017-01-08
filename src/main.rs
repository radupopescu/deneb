extern crate crypto;
extern crate merkle;

use crypto::digest::Digest;
use crypto::sha2::Sha256;

use merkle::errors::*;
use merkle::tree::{Hashable, Tree};

struct Dummy {
    val: &'static str,
}

impl Dummy {
    fn new(v: &'static str) -> Dummy {
        Dummy { val: v }
    }
}

impl Hashable for Dummy {
    fn hash(&self) -> Result<Vec<u8>, MerkleError> {
        let mut hasher = Sha256::new();
        hasher.input_str(self.val);
        Ok(Vec::from(hasher.result_str()))
    }
}

fn main() {
    println!("Welcome to Deneb!");

    println!("Building a dummy Merkle tree");

    let dummies = vec![Dummy::new("ala"),
                       Dummy::new("bala"),
                       Dummy::new("portocala"),
                       Dummy::new("dala"),
                       Dummy::new("eala"),
                       Dummy::new("kalle")];

    let tree = Tree::from(&dummies);


}