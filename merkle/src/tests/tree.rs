extern crate crypto;

use super::super::tree::*;
use super::super::errors::*;

fn get_hash(values: &[&str]) -> Vec<u8> {
    if let Ok(tree) = Tree::new(values) {
        if let Ok(hash) = tree.root_hash() {
            hash
        } else {
            panic!("Could not compute root hash of tree.");
        }
    } else {
        panic!("Could not build Merkle tree.");
    }
}

#[test]
fn empty_tree() {
    let err = Tree::new(&[] as &[&str]);
    match err {
        Err(MerkleError::EmptyInput) => (),
        _ => {
            panic!("Tree::new(&[]) should return MerkleError::EmptyInput");
        }
    }
}

#[test]
fn tree_of_single_leaf() {
    assert_ne!(get_hash(&["ala"]), []);
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