extern crate copy_dir;
#[macro_use]
extern crate failure;
extern crate log;
extern crate quickcheck;
extern crate rand;
extern crate tempdir;
extern crate uuid;

extern crate deneb_core;
extern crate deneb_fuse;

use copy_dir::copy_dir;
use quickcheck::{QuickCheck, StdGen};
use tempdir::TempDir;

use std::fs::create_dir_all;
use std::path::Path;

mod common;

use common::*;

use deneb_core::errors::DenebResult;
use deneb_core::populate_with_dir;
use deneb_core::catalog::{CatalogBuilder, LmdbCatalogBuilder, MemCatalogBuilder};
use deneb_core::engine::{start_engine, start_engine_prebuilt};
use deneb_core::store::{DiskStoreBuilder, MemStoreBuilder, StoreBuilder};
use deneb_fuse::fs::{Fs, Session};

const DEFAULT_CHUNK_SIZE: usize = 4_194_304; // 4MB default;

#[derive(Clone, Copy)]
enum TestType {
    InMemory,
    OnDisk,
}

// Function to generate an input dir tree
fn make_test_dir_tree(prefix: &Path) -> DenebResult<DirTree> {
    let root = prefix.join("input");
    println!("Root: {:?}", root);

    let entries = vec![
        DirEntry::File("a.txt".to_owned(), b"hello\n".to_vec()),
        DirEntry::Dir(
            "dir1".to_owned(),
            vec![
                DirEntry::File("b.txt".to_owned(), b"is it me\n".to_vec()),
                DirEntry::File("c.txt".to_owned(), b"you're looking\n".to_vec()),
            ],
        ),
        DirEntry::Dir(
            "dir2".to_owned(),
            vec![
                DirEntry::Dir(
                    "dir3".to_owned(),
                    vec![DirEntry::File("c.txt".to_owned(), b"for?\n".to_vec())],
                ),
            ],
        ),
    ];

    let dt = DirTree::with_entries(root, entries);

    // Initialize input data
    assert!(dt.show().is_ok());
    assert!(dt.create().is_ok());

    Ok(dt)
}

// Initialize a Deneb repo with the input directory
fn init_test<'a>(
    test_type: TestType,
    input: &Path,
    prefix: &Path,
    chunk_size: usize,
) -> DenebResult<Session<'a>> {
    let mount_point = prefix.join("mount");
    create_dir_all(&mount_point)?;
    let work_dir = prefix.join("internal");

    let handle = match test_type {
        TestType::InMemory => {
            // The paths given to the in-memory builders doesn't matter
            let mut store = MemStoreBuilder.at_dir(&work_dir, chunk_size)?;
            let mut catalog = MemCatalogBuilder.create(&work_dir)?;
            populate_with_dir(&mut catalog, &mut store, input, chunk_size)?;
            start_engine_prebuilt(catalog, store, 1000)
        }
        TestType::OnDisk => start_engine(
            &LmdbCatalogBuilder,
            &DiskStoreBuilder,
            &work_dir,
            Some(input.to_owned()),
            chunk_size,
            1000,
        ),
    }?;
    let file_system = Fs::new(handle);
    unsafe { file_system.spawn_mount(&mount_point, &[]) }
}

// Simple integration test
//
// Use a previously generated DirTree to populate a Deneb repository.
// Copy all the files back out of the Deneb repository and compare with the originals.
fn check_inout(
    test_type: TestType,
    dir: &DirTree,
    prefix: &Path,
    chunk_size: usize,
) -> DenebResult<()> {
    // Create and mount the deneb repo
    let _session = init_test(test_type, dir.root.as_path(), prefix, chunk_size)?;

    // Copy the contents of the Deneb repository to a new directory
    let output_dir = prefix.join("output");
    copy_dir(&prefix.join("mount"), output_dir.as_path())?;

    // Compare the input directory tree with the one copied out of the Deneb repo
    dir.compare(output_dir.as_path())
}

fn single_fuse_test(test_type: TestType, chunk_size: usize) {
    let tmp = TempDir::new("/tmp/deneb_single_fuse_test");
    assert!(tmp.is_ok());
    if let Ok(prefix) = tmp {
        let dt = make_test_dir_tree(prefix.path());
        assert!(dt.is_ok());
        if let Ok(dt) = dt {
            assert!(check_inout(test_type, &dt, prefix.path(), chunk_size).is_ok());
        }

        // Explicit cleanup
        assert!(prefix.close().is_ok());
    }
}

#[ignore]
#[test]
fn single_chunk_per_file_memory() {
    single_fuse_test(TestType::InMemory, DEFAULT_CHUNK_SIZE); // test with 4MB chunk size (1 chunk per file)
}

#[ignore]
#[test]
fn single_chunk_per_file_disk() {
    single_fuse_test(TestType::OnDisk, DEFAULT_CHUNK_SIZE); // test with 4MB chunk size (1 chunk per file)
}

#[ignore]
#[test]
fn multiple_chunks_per_file_memory() {
    single_fuse_test(TestType::InMemory, 4); // test with 4B chunk size (multiple chunks per file are needed)
}

#[ignore]
#[test]
fn multiple_chunks_per_file_disk() {
    single_fuse_test(TestType::OnDisk, 4); // test with 4B chunk size (multiple chunks per file are needed)
}

#[ignore]
#[test]
fn prop_inout_unchanged() {
    fn inout_unchanged(mut dt: DirTree) -> bool {
        let tmp = TempDir::new("/tmp/deneb_fuse_prop_inout");
        if !tmp.is_ok() {
            return false;
        }
        if let Ok(prefix) = tmp {
            dt.root = prefix.path().to_owned();

            let _ = dt.show();
            let _ = dt.create();

            let check_result =
                check_inout(TestType::InMemory, &dt, prefix.path(), DEFAULT_CHUNK_SIZE);
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
        let tmp = TempDir::new("/tmp/deneb_fuse_prop_inout");
        if !tmp.is_ok() {
            return false;
        }
        if let Ok(prefix) = tmp {
            dt.root = prefix.path().to_owned();

            let _ = dt.show();
            let _ = dt.create();

            let check_result =
                check_inout(TestType::OnDisk, &dt, prefix.path(), DEFAULT_CHUNK_SIZE);
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