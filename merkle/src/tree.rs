use crypto::digest::Digest;
use crypto::sha2::Sha256;

use std::rc::Rc;

use errors;

pub trait Hashable {
    fn hash(&self) -> Result<Vec<u8>, errors::MerkleError>;
}

pub struct Tree {
    root: Rc<Element>,
}

enum Element {
    Empty,
    Leaf { hash: Vec<u8> },
    Node {
        hash: Vec<u8>,
        left: Rc<Element>,
        right: Rc<Element>,
    },
}

impl Hashable for Element {
    fn hash(&self) -> Result<Vec<u8>, errors::MerkleError> {
        match *self {
            Element::Empty => Ok(Vec::from("")),
            Element::Leaf { ref hash } => Ok(hash.clone()),
            Element::Node { ref left, ref right, .. } => {
                let mut hasher = Sha256::new();
                let h1 = String::from_utf8(left.hash()?)?;
                let h2 = String::from_utf8(right.hash()?)?;
                let h3 = h1 + h2.as_str();
                hasher.input_str(h3.as_str());
                Ok(Vec::from(hasher.result_str()))
            }
        }
    }
}

impl<'a> Hashable for &'a str {
    fn hash(&self) -> Result<Vec<u8>, errors::MerkleError> {
        let mut hasher = Sha256::new();
        hasher.input_str(self);
        Ok(Vec::from(hasher.result_str()))
    }
}

impl Tree {
    pub fn new() -> Tree {
        Tree { root: Rc::new(Element::Empty) }
    }

    pub fn from<T: Hashable>(vals: &[T]) -> Result<Tree, errors::MerkleError> {
        let mut nodes = Vec::new();
        for v in vals.into_iter() {
            let h = v.hash()?;
            nodes.push(Rc::new(Element::Leaf { hash: h }));
        }
        if nodes.len() % 2 != 0 {
            nodes.push(Rc::new(Element::Empty));
        }
        let mut current_length = nodes.len();
        while current_length > 1 {
            for i in 0..(current_length / 2) {
                nodes[i] = Self::reduce(&nodes[i], &nodes[i + 1]);
            }
            nodes.resize(current_length / 2, Rc::new(Element::Empty));
            if nodes.len() > 1 && nodes.len() % 2 != 0 {
                nodes.push(Rc::new(Element::Empty));
            }
            current_length = nodes.len();
        }
        if nodes.len() > 0 {
            Ok(Tree { root: nodes[0].clone() })
        } else {
            Ok(Tree { root: Rc::new(Element::Empty) })
        }
    }

    pub fn root_hash(&self) -> Result<Vec<u8>, errors::MerkleError> {
        self.root.hash()
    }

    fn reduce(n1: &Rc<Element>, n2: &Rc<Element>) -> Rc<Element> {
        match (&**n1, &**n2) {
            (&Element::Empty, &Element::Empty) => Rc::new(Element::Empty),
            (&Element::Leaf { .. }, &Element::Empty) => n1.clone(),
            (&Element::Leaf { hash: ref h1 }, &Element::Leaf { hash: ref h2 }) => {
                let mut hasher = Sha256::new();
                let h1 = String::from_utf8(h1.clone()).unwrap();
                let h2 = String::from_utf8(h2.clone()).unwrap();
                let h = h1 + h2.as_str();
                hasher.input_str(h.as_str());
                Rc::new(Element::Node {
                    hash: Vec::from(hasher.result_str()),
                    left: n1.clone(),
                    right: n2.clone(),
                })
            }
            (&Element::Node { .. }, &Element::Empty) => n1.clone(),
            (&Element::Node { hash: ref h1, .. }, &Element::Node { hash: ref h2, .. }) => {
                let mut hasher = Sha256::new();
                let h1 = String::from_utf8(h1.clone()).unwrap();
                let h2 = String::from_utf8(h2.clone()).unwrap();
                let h = h1 + h2.as_str();
                hasher.input_str(h.as_str());
                Rc::new(Element::Node {
                    hash: Vec::from(hasher.result_str()),
                    left: n1.clone(),
                    right: n2.clone(),
                })
            }
            _ => {
                panic!("This shouldn't happen!");
            }
        }
    }
}

impl Default for Tree {
    fn default() -> Self {
        Self::new()
    }
}