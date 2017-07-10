extern crate lmdb_rs;

use lmdb_rs::core::{DbCreate, EnvBuilder, EnvCreateNoSubDir};

use std::fs::remove_dir_all;

fn main() {
    println!("Hello from the LMDB playground!");

    let _ = remove_dir_all("/tmp/test-lmdb");
    let _ = remove_dir_all("/tmp/test-lmdb-lock");

    // Open the environment
    let env = EnvBuilder::new()
        .flags(EnvCreateNoSubDir)
        .max_dbs(100)
        .open("/tmp/test-lmdb", 0o600).unwrap();

    // Create a named database
    let db_hd = env.create_db("test", DbCreate).unwrap();

    // Start a write transaction
    let writer = env.new_transaction().unwrap();
    {
        // Write something with mdb_put
        let db = writer.bind(&db_hd);
        db.set(&"hello", &"world").unwrap();
    }

    // Commit transaction
    writer.commit().unwrap();

    // Create a read-only transaction
    let mut reader = env.get_reader().unwrap();
    {
        let db = reader.bind(&db_hd);

        // Read something with mdb_get
        let key = "hello";
        let val = db.get::<&str>(&key).unwrap();

        println!("{} -> {}", key, val);
    }
    reader.reset();

    // Start a write transaction
    let writer = env.new_transaction().unwrap();
    {
        // Write something with mdb_put
        let db = writer.bind(&db_hd);
        db.set(&"hello2", &"world2").unwrap();
    }

    // Commit transaction
    writer.commit().unwrap();

    reader.renew().unwrap();
    {
        let db = reader.bind(&db_hd);

        // Read something with mdb_get
        let key = "hello2";
        let val = db.get::<&str>(&key).unwrap();

        println!("{} -> {}", key, val);
    }
}
