extern crate crypto;

use crypto::digest::Digest;
use crypto::sha2::Sha256;

use super::super::tree::*;
use super::super::errors::*;

impl<'a> Hashable for &'a str {
    fn hash(&self) -> Result<Vec<u8>, MerkleError> {
        let mut hasher = Sha256::new();
        hasher.input_str(self);
        Ok(Vec::from(hasher.result_str()))
    }
}

fn get_hash(values: &[&str]) -> Vec<u8> {
    let mut result = Vec::new();
    if let Ok(tree) = Tree::from(values) {
        if let Ok(hash) = tree.root_hash() {
            result = hash;
        } else {
            panic!("Could not compute root hash of tree.");
        }
    } else {
        panic!("Could not build Merkle tree.");
    }
    result
}

#[test]
fn empty_tree() {
    assert_eq!(get_hash(&[]), []);
}

#[test]
fn tree_of_two_empties() {
    assert_eq!(get_hash(&["", ""]), []);
}

#[test]
fn tree_of_empty_and_leaf() {
    assert_ne!(get_hash(&["ala", ""]), []);
}

#[test]
fn tree_of_two_leaves() {
    assert_ne!(get_hash(&["ala", "bala"]), []);
}

#[test]
fn complex_tree() {
    assert_ne!(get_hash(&["ala", "bala", "portocala", "dala", "eala", "fala", "kalle"]),
               []);
}