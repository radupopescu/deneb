use std::collections::HashMap;

use hash::ContentHash;

pub trait Store {
    fn get(&self, hash: &ContentHash) -> Option<&[u8]>;

    fn put(&mut self, hash: ContentHash, contents: &[u8]);
}

#[derive(Default)]
pub struct HashMapStore {
    objects: HashMap<ContentHash, Vec<u8>>,
}
impl HashMapStore {
    pub fn new() -> HashMapStore {
        Self::default()
    }
}

impl Store for HashMapStore {
    fn get(&self, hash: &ContentHash) -> Option<&[u8]> {
        self.objects.get(hash).map(|v| v.as_slice())
    }

    fn put(&mut self, hash: ContentHash, contents: &[u8]) {
        self.objects.entry(hash).or_insert_with(|| contents.to_vec());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_put_get() {
        let mut store: HashMapStore = HashMapStore::new();
        let k1 = "some_key".as_ref();
        let v1: Vec<u8> = vec![1,2,3];
        store.put(ContentHash::from(k1), v1.as_slice());
        if let Some(v2) = store.get(&ContentHash::from(k1)) {
            println!("v1 = {:?}, v2 = {:?}", v1, v2);
        } else {
            println!("store.get returned None");
        }
    }
}
