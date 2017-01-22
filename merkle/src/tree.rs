use crypto::digest::Digest;
use crypto::sha2::Sha256;

use std::rc::Rc;

use errors;

pub trait Hashable {
    fn hash(&self) -> Result<Vec<u8>, errors::MerkleError>;
}

pub struct Tree {
    root: Element,
}

#[derive(Clone)]
struct Children {
    left: Rc<Element>,
    right: Rc<Element>,
}

#[derive(Clone)]
struct Element {
    hash: Vec<u8>,
    children: Option<Children>,
}

/// Recursively build the Merkle tree
fn build(vals: &[Element]) -> Element {
    match vals.len() {
        1 => vals[0].clone(),
        2 => reduce(&vals[0], &vals[1]),
        _ => {
            reduce(&build(&vals[0..vals.len() / 2]),
                   &build(&vals[vals.len() / 2 + 1..]))
        }
    }
}

fn reduce(n1: &Element, n2: &Element) -> Element {
    let (&Element { hash: ref h1, .. }, &Element { hash: ref h2, .. }) = (n1, n2);
    Element {
        hash: combine_hashes(h1, h2),
        children: Some(Children {
            left: Rc::new(n1.clone()),
            right: Rc::new(n2.clone()),
        }),
    }
}

fn combine_hashes(h1: &Vec<u8>, h2: &Vec<u8>) -> Vec<u8> {
    let mut hasher = Sha256::new();
    let h1 = String::from_utf8(h1.clone()).unwrap();
    let h2 = String::from_utf8(h2.clone()).unwrap();
    let h = h1 + h2.as_str();
    hasher.input_str(h.as_str());
    Vec::from(hasher.result_str())
}

impl<'a> Hashable for &'a str {
    fn hash(&self) -> Result<Vec<u8>, errors::MerkleError> {
        let mut hasher = Sha256::new();
        hasher.input_str(self);
        Ok(Vec::from(hasher.result_str()))
    }
}

impl Tree {
    pub fn new<T: Hashable>(vals: &[T]) -> Result<Tree, errors::MerkleError> {
        if vals.is_empty() {
            Err(errors::MerkleError::EmptyInput)
        } else {
            let mut nodes = Vec::new();
            for v in vals.into_iter() {
                let h = v.hash()?;
                nodes.push(Element {
                    hash: h,
                    children: None,
                });
            }
            Ok(Tree { root: build(&nodes[..]) })
        }
    }

    pub fn root_hash(&self) -> &Vec<u8> {
        &(self.root.hash)
    }
}