#[derive(Debug,Hash,Eq,PartialEq,Default)]
pub struct ContentHash {
    hash: Vec<u8>,
}
impl ContentHash {
    pub fn new() -> ContentHash {
        ContentHash { hash: vec![] }
    }
}

impl<'a> From<&'a [u8]> for ContentHash {
    fn from(other: &'a [u8]) -> Self {
        ContentHash { hash: other.to_owned() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_from_str() {
        let _ = ContentHash::new();
        let _ = ContentHash::from("non-empty".as_ref());
    }
}
