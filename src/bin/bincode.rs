extern crate serde;
extern crate serde_bytes;
#[macro_use]
extern crate serde_derive;
extern crate bincode;

use bincode::{serialize, deserialize, Infinite};

use std::collections::HashMap;

mod sort_hm {
    use serde::{Serialize, Serializer};
    use std::collections::{BTreeMap, HashMap};

    pub fn serialize<S>(hm: &HashMap<String, u64>, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer {
        let ordered_map = hm.iter().collect::<BTreeMap<_,_>>();
        ordered_map.serialize(serializer)
    }
}

#[derive(Serialize, Deserialize, PartialEq)]
struct Entity {
    x: f32,
    y: f32,
    #[serde(serialize_with="sort_hm::serialize")]
    hm: HashMap<String, u64>,
}

#[derive(Serialize, Deserialize, PartialEq)]
struct World(Vec<Entity>);

fn main() {
    let mut hm = HashMap::new();
    hm.insert("portocala".to_owned(), 3);
    hm.insert("ala".to_owned(), 1);
    hm.insert("bala".to_owned(), 2);
    let world = World(vec![Entity { x: 0.0, y: 4.0, hm: hm.clone() }, Entity { x: 10.0, y: 20.5, hm: hm.clone() }]);

    let encoded: Vec<u8> = serialize(&world, Infinite).unwrap();

    // 8 bytes for the length of the vector, 4 bytes per float.
    assert_eq!(encoded.len(), 168);

    println!("Encoded: {:?}", encoded);

    let decoded: World = deserialize(&encoded[..]).unwrap();

    assert!(world == decoded);
}
