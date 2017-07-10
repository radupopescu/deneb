extern crate lmdb;

use lmdb::{DatabaseFlags, Environment, Transaction, WriteFlags, NO_SUB_DIR};

use std::fs::remove_dir_all;
use std::path::Path;

fn main() {
    println!("Hello from the LMDB playground!");

    let _ = remove_dir_all("/tmp/test-lmdb");
    let _ = remove_dir_all("/tmp/test-lmdb-lock");

    // Open the environment
    let env = Environment::new()
        .set_flags(NO_SUB_DIR)
        .set_max_dbs(100)
        .open_with_permissions(&Path::new("/tmp/test-lmdb"), 0o600)
        .unwrap();

    // Create a named database
    let db = env.create_db(Some("test"), DatabaseFlags::empty())
        .unwrap();

    // Start a write transaction
    let mut writer = env.begin_rw_txn().unwrap();
    writer
        .put(db, &"hello", &"world", WriteFlags::empty())
        .unwrap();

    // Commit transaction
    writer.commit().unwrap();

    // Create a read-only transaction
    let reader = env.begin_ro_txn().unwrap();

    {
        let key = "hello";
        let val = reader.get(db, &key).unwrap();

        println!("{} -> {:?}", key, val);
    }

    let inactive = reader.reset();

    // Start a write transaction
    let mut writer = env.begin_rw_txn().unwrap();

    writer
        .put(db, &"hello2", &"world2", WriteFlags::empty())
        .unwrap();

    // Commit transaction
    writer.commit().unwrap();

    let reader = inactive.renew().unwrap();

    // Read something with mdb_get
    let key = "hello2";
    let val = reader.get(db, &key).unwrap();

    println!("{} -> {:?}", key, val);
}
