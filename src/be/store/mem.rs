use std::collections::HashMap;

use be::cas::Digest;

use super::Store;

#[derive(Default)]
pub struct MemStore {
    objects: HashMap<Digest, Vec<u8>>,
}
impl MemStore {
    pub fn new() -> MemStore {
        Self::default()
    }

    pub fn show_stats(&self) {
        info!("MemStore: number of objects: {}", self.objects.len());
    }
}

impl Store for MemStore {
    fn get(&self, hash: &Digest) -> Option<&[u8]> {
        self.objects.get(hash).map(|v| v.as_slice())
    }

    fn put(&mut self, hash: Digest, contents: &[u8]) {
        self.objects.entry(hash).or_insert_with(|| contents.to_vec());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use be::cas::hash;

    #[test]
    fn memstore_create_put_get() {
        let mut store: MemStore = MemStore::new();
        let k1 = "some_key".as_ref();
        let v1: Vec<u8> = vec![1,2,3];
        store.put(hash(k1), v1.as_slice());
        if let Some(v2) = store.get(&hash(k1)) {
            println!("v1 = {:?}, v2 = {:?}", v1, v2);
        } else {
            println!("store.get returned None");
        }
    }
}
