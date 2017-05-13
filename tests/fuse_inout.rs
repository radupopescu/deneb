extern crate copy_dir;
extern crate deneb;
#[macro_use]
extern crate error_chain;
extern crate fuse;
extern crate quickcheck;
extern crate rand;
extern crate rust_sodium;
extern crate tempdir;
extern crate uuid;

use quickcheck::{QuickCheck, StdGen};
use fuse::BackgroundSession;
use tempdir::TempDir;

use std::path::Path;
use std::fs::create_dir;

mod common;

use common::*;

use deneb::catalog::HashMapCatalog;
use deneb::errors::*;
use deneb::fs::Fs;
use deneb::store::HashMapStore;

// Function to generate an input dir tree
fn make_test_dir_tree(prefix: &Path) -> Result<DirTree> {
    let root = prefix.join("input");
    println!("Root: {:?}", root);

    let entries = vec![DirEntry::File("a.txt".to_owned(), b"hello\n".to_vec()),
                       DirEntry::Dir("dir1".to_owned(),
                                     vec![DirEntry::File("b.txt".to_owned(),
                                                         b"is it me\n".to_vec()),
                                          DirEntry::File("c.txt".to_owned(),
                                                         b"you're looking\n".to_vec())]),
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
fn init_hashmap_repo<'a>(input: &Path, mount_point: &Path) -> Result<BackgroundSession<'a>> {
    let mut store = HashMapStore::new();
    let catalog = HashMapCatalog::with_dir(input, &mut store)?;
    let file_system = Fs::new(catalog, store);
    unsafe { fuse::spawn_mount(file_system, &mount_point.to_owned(), &[]).map_err(|e| e.into()) }
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
fn check_inout(dir: &DirTree, prefix: &Path) -> Result<()> {
    // Create and mount the deneb repo
    let mount_point = prefix.join("mount");
    create_dir(mount_point.as_path())?;
    let _session = init_hashmap_repo(dir.root.as_path(), mount_point.as_path())?;

    // Copy the contents of the Deneb repository to a new directory
    let output_dir = prefix.join("output");
    copy_dir_tree(mount_point.as_path(), output_dir.as_path())?;

    // Compare the input directory tree with the one copied out of the Deneb repo
    dir.compare(output_dir.as_path())
}

#[test]
fn single_fuse_hashmap_inout() {
    let tmp = TempDir::new("/tmp/deneb_test");
    assert!(tmp.is_ok());
    if let Ok(prefix) = tmp {
        let dt = make_test_dir_tree(prefix.path());
        assert!(dt.is_ok());
        if let Ok(dt) = dt {
            assert!(check_inout(&dt, prefix.path()).is_ok());
        }

        // Explicit cleanup
        assert!(prefix.close().is_ok());
    }
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

            let check_result = check_inout(&dt, prefix.path());
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
