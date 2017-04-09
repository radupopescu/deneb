use std::collections::HashMap;
use std::hash::Hash;

pub trait Store<K> {
    fn get(&self, hash: &K) -> Option<&[u8]>;

    fn put(&mut self, hash: K, contents: &[u8]);
}

pub struct HashMapStore<K> {
    objects: HashMap<K, Vec<u8>>,
}

impl<K> HashMapStore<K> where K: Hash + Eq + PartialEq {
    pub fn new() -> HashMapStore<K> {
        HashMapStore { objects: HashMap::new() }
    }
}

impl<K> Store<K> for HashMapStore<K> where K: Hash + Eq + PartialEq {
    fn get(&self, hash: &K) -> Option<&[u8]> {
        self.objects.get(&hash).map(|v| v.as_slice())
    }

    fn put(&mut self, hash: K, contents: &[u8]) {
        self.objects.entry(hash).or_insert(contents.to_vec());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_put_get() {
        let mut store: HashMapStore<String> = HashMapStore::new();
        let k1 = "some_key";
        let v1: Vec<u8> = vec![1,2,3];
        store.put(k1.to_owned(), v1.as_slice());
        if let Some(v2) = store.get("some_key".to_owned()) {
            println!("v1 = {:?}, v2 = {:?}", v1, v2);
        } else {
            println!("store.get returned None");
        }
    }
}
