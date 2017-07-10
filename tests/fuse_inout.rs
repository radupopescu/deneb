#![cfg(feature = "fuse")]

extern crate copy_dir;
extern crate deneb;
#[macro_use]
extern crate error_chain;
extern crate quickcheck;
extern crate rand;
extern crate rust_sodium;
extern crate tempdir;
extern crate uuid;

use quickcheck::{QuickCheck, StdGen};
use tempdir::TempDir;

use std::path::Path;
use std::fs::create_dir;

mod common;

use common::*;

use deneb::be::catalog::{Catalog, CatalogBuilder, LmdbCatalogBuilder, MemCatalog};
use deneb::be::populate_with_dir;
use deneb::be::store::{Store, StoreBuilder, DiskStoreBuilder, MemStore};
use deneb::common::errors::*;
use deneb::fe::fuse::{Fs, Session};
use deneb::fe::fuse::DEFAULT_CHUNK_SIZE;

// Function to generate an input dir tree
fn make_test_dir_tree(prefix: &Path) -> Result<DirTree> {
    let root = prefix.join("input");
    println!("Root: {:?}", root);

    let entries =
        vec![DirEntry::File("a.txt".to_owned(), b"hello\n".to_vec()),
             DirEntry::Dir("dir1".to_owned(),
                           vec![DirEntry::File("b.txt".to_owned(), b"is it me\n".to_vec()),
                                DirEntry::File("c.txt".to_owned(), b"you're looking\n".to_vec())]),
             DirEntry::Dir("dir2".to_owned(),
                           vec![DirEntry::Dir("dir3".to_owned(),
                                              vec![DirEntry::File("c.txt".to_owned(),
                                                                  b"for?\n".to_vec())])])];

    let dt = DirTree::with_entries(root, entries);

    // Initialize input data
    assert!(dt.show().is_ok());
    assert!(dt.create().is_ok());

    Ok(dt)
}


// Initialize a Deneb repo with the input directory
fn init_repo<'a, C, S>(mut catalog: C,
                       mut store: S,
                       input: &Path,
                       mount_point: &Path,
                       chunk_size: usize)
                       -> Result<Session<'a>>
    where C: 'a + Catalog + Send,
          S: 'a + Store + Send
{
    populate_with_dir(&mut catalog, &mut store, input, chunk_size)?;
    let file_system = Fs::new(catalog, store);
    unsafe { file_system.spawn_mount(&mount_point.to_owned(), &[]) }
}

// Copy the contents of the Deneb repo out to a new location
fn copy_dir_tree(source: &Path, dest: &Path) -> Result<()> {
    copy_dir::copy_dir(source, dest)?;
    Ok(())
}

// Simple integration test
//
// Use a previously generated DirTree to populate a Deneb repository.
// Copy all the files back out of the Deneb repository and compare with the originals.
fn check_inout<C, S>(catalog: C,
                     store: S,
                     dir: &DirTree,
                     prefix: &Path,
                     chunk_size: usize)
                     -> Result<()>
    where C: Catalog + Send,
          S: Store + Send
{
    // Create and mount the deneb repo
    let mount_point = prefix.join("mount");
    create_dir(mount_point.as_path())?;
    let _session = init_repo(catalog,
                             store,
                             dir.root.as_path(),
                             mount_point.as_path(),
                             chunk_size)?;

    // Copy the contents of the Deneb repository to a new directory
    let output_dir = prefix.join("output");
    copy_dir_tree(mount_point.as_path(), output_dir.as_path())?;

    // Compare the input directory tree with the one copied out of the Deneb repo
    dir.compare(output_dir.as_path())
}

enum TestType {
    InMemory,
    OnDisk,
}

fn single_fuse_test(test_type: &TestType, chunk_size: usize) {
    let tmp = TempDir::new("/tmp/deneb_test_fuse_inout");
    assert!(tmp.is_ok());
    if let Ok(prefix) = tmp {
        let dt = make_test_dir_tree(prefix.path());
        assert!(dt.is_ok());
        if let Ok(dt) = dt {
            match *test_type {
                TestType::InMemory => {
                    let catalog = MemCatalog::new();
                    let store = MemStore::new();
                    assert!(check_inout(catalog, store, &dt, prefix.path(), chunk_size).is_ok());
                }
                TestType::OnDisk => {
                    let catalog_path = prefix.path().join("current_catalog");
                    let cb = LmdbCatalogBuilder;
                    let catalog = cb.create(catalog_path);
                    assert!(catalog.is_ok());
                    if let Ok(catalog) = catalog {
                        let store_path = prefix.path().join("internal");
                        let sb = DiskStoreBuilder;
                        let store = sb.at_dir(store_path);
                        assert!(store.is_ok());
                        if let Ok(store) = store {
                            assert!(check_inout(catalog, store, &dt, prefix.path(), chunk_size)
                                        .is_ok());
                        }
                    }
                }
            }
        }

        // Explicit cleanup
        assert!(prefix.close().is_ok());
    }
}

#[test]
fn single_chunk_per_file_memory() {
    single_fuse_test(&TestType::InMemory, DEFAULT_CHUNK_SIZE); // test with 4MB chunk size (1 chunk per file)
}

#[test]
fn single_chunk_per_file_disk() {
    single_fuse_test(&TestType::OnDisk, DEFAULT_CHUNK_SIZE); // test with 4MB chunk size (1 chunk per file)
}

#[test]
fn multiple_chunks_per_file_memory() {
    single_fuse_test(&TestType::InMemory, 4); // test with 4B chunk size (multiple chunks per file are needed)
}

#[test]
fn multiple_chunks_per_file_disk() {
    single_fuse_test(&TestType::OnDisk, 4); // test with 4B chunk size (multiple chunks per file are needed)
}

#[test]
fn prop_inout_unchanged() {
    fn inout_unchanged(mut dt: DirTree) -> bool {
        let tmp = TempDir::new("/tmp/deneb_test");
        if !tmp.is_ok() {
            return false;
        }
        if let Ok(prefix) = tmp {
            dt.root = prefix.path().to_owned();

            let _ = dt.show();
            let _ = dt.create();

            let catalog = MemCatalog::new();
            let store = MemStore::new();
            let check_result = check_inout(catalog, store, &dt, prefix.path(), DEFAULT_CHUNK_SIZE);
            if !check_result.is_ok() {
                println!("Check failed: {:?}", check_result);
                return false;
            }

            // Explicit cleanup
            if !prefix.close().is_ok() {
                return false;
            }
        }
        true
    }
    QuickCheck::new()
        .tests(50)
        .gen(StdGen::new(rand::thread_rng(), 5))
        .quickcheck(inout_unchanged as fn(DirTree) -> bool);
}

#[ignore]
#[test]
fn prop_inout_unchanged_disk_slow() {
    fn inout_unchanged(mut dt: DirTree) -> bool {
        let tmp = TempDir::new("/tmp/deneb_test");
        if !tmp.is_ok() {
            return false;
        }
        if let Ok(prefix) = tmp {
            dt.root = prefix.path().to_owned();

            let _ = dt.show();
            let _ = dt.create();

            let catalog_path = prefix.path().join("current_catalog");
            let cb = LmdbCatalogBuilder;
            let catalog = cb.create(catalog_path);
            if let Ok(catalog) = catalog {
                let store_path = prefix.path().join("internal");
                let sb = DiskStoreBuilder;
                let store = sb.at_dir(store_path);
                assert!(store.is_ok());
                if let Ok(store) = store {
                    let check_result =
                        check_inout(catalog, store, &dt, prefix.path(), DEFAULT_CHUNK_SIZE);
                    if !check_result.is_ok() {
                        println!("Check failed: {:?}", check_result);
                        return false;
                    }
                } else {
                    println!("Error creating disk store");
                    return false;
                }
            } else {
                println!("Error creating LMDB catalog");
                return false;
            }

            // Explicit cleanup
            if !prefix.close().is_ok() {
                return false;
            }
        }
        true
    }
    QuickCheck::new()
        .tests(50)
        .gen(StdGen::new(rand::thread_rng(), 5))
        .quickcheck(inout_unchanged as fn(DirTree) -> bool);
}
