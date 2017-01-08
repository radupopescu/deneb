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
            Element::Node { ref hash, ref left, ref right } => {
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

impl Tree {
    pub fn new() -> Tree {
        Tree { root: Rc::new(Element::Empty) }
    }

    pub fn from<T: Hashable>(vals: &Vec<T>) -> Result<Tree, errors::MerkleError> {
        let mut nodes = Vec::new();
        for v in vals.into_iter() {
            let h = v.hash()?;
            nodes.push(Rc::new(Element::Leaf { hash: h }));
        }
        if nodes.len() % 2 != 0 {
            nodes.push(Rc::new(Element::Empty));
        }
        let mut current_length = nodes.len();
        println!("Current length: {}", current_length);
        while current_length > 1 {
            println!("Current length: {}", current_length);
            for i in 0..(current_length / 2) {
                nodes[i] = Self::reduce(&nodes[i], &nodes[i + 1]);
            }
            nodes.resize(current_length / 2, Rc::new(Element::Empty));
            if nodes.len() > 1 && nodes.len() % 2 != 0 {
                nodes.push(Rc::new(Element::Empty));
            }
            current_length = nodes.len();
        }
        Ok(Self::new())
    }

    fn reduce(n1: &Rc<Element>, n2: &Rc<Element>) -> Rc<Element> {
        match (&**n1, &**n2) {
            (&Element::Empty, &Element::Empty) => {
                println!("Two empties");
                Rc::new(Element::Empty)
            }
            (&Element::Leaf { .. }, &Element::Empty) => {
                println!("Leaf and empty");
                n1.clone()
            }
            (&Element::Leaf { hash: ref h1 }, &Element::Leaf { hash: ref h2 }) => {
                println!("Two leaves");
                Rc::new(Element::Empty)
            }
            (&Element::Node { .. }, &Element::Empty) => {
                println!("Node and leaf");
                n1.clone()
            }
            (&Element::Node { .. }, &Element::Node { .. }) => {
                println!("Two nodes");
                Rc::new(Element::Empty)
            }
            _ => {
                println!("This shouldn't happen!");
                Rc::new(Element::Empty)
            }
        }
    }
}

impl Default for Tree {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
