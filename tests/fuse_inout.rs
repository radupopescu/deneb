extern crate copy_dir;
extern crate deneb;
#[macro_use]
extern crate error_chain;
extern crate fuse;
extern crate quickcheck;
extern crate rust_sodium;
extern crate tempdir;

// use quickcheck::QuickCheck;
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

    let entries = vec![
        DirEntry::File("a.txt".to_owned(), "hello\n".as_bytes().to_vec()),
        DirEntry::Dir(
            "dir1".to_owned(),
            vec![DirEntry::File("b.txt".to_owned(), "is it me\n".as_bytes().to_vec()),
                 DirEntry::File("c.txt".to_owned(), "you're looking\n".as_bytes().to_vec())]),
        DirEntry::Dir(
            "dir2".to_owned(),
            vec![DirEntry::Dir("dir3".to_owned(),
                               vec![DirEntry::File("c.txt".to_owned(),
                                                   "for?\n".as_bytes().to_vec())])])];
    Ok(DirTree::with_entries(root, entries))
}


// Initialize a Deneb repo with the input directory
fn init_hashmap_repo<'a>(input: &Path, mount_point: &Path) -> Result<BackgroundSession<'a>> {
    let mut store = HashMapStore::new();
    let catalog = HashMapCatalog::with_dir(input, &mut store)?;
    let file_system = Fs::new(catalog, store);
    unsafe { fuse::spawn_mount(file_system,
                               &mount_point.to_owned(),
                               &[]).map_err(|e| e.into()) }
}

// Copy the contents of the Deneb repo out to a new location
fn copy_dir_tree(source: &Path, dest: &Path) -> Result<()> {
    copy_dir::copy_dir(source, dest)?;
    Ok(())
}

// Simple integration test
//
// Generate an arbitrary directory tree and use it to populate a Deneb repository.
// Copy all the files back out of the Deneb repository and compare with the originals.
fn check_inout(dir: DirTree, prefix: &Path) {
    // Initialize input data
    assert!(dir.show().is_ok());
    assert!(dir.create().is_ok());

    // Create and mount the deneb repo
    let mount_point = prefix.join("mount");
    assert!(create_dir(mount_point.as_path()).is_ok());
    let session = init_hashmap_repo(dir.root.as_path(), mount_point.as_path());
    assert!(session.is_ok());

    // Copy the contents of the Deneb repository to a new directory
    let output_dir = prefix.join("output");
    assert!(copy_dir_tree(mount_point.as_path(), output_dir.as_path()).is_ok());

    // Compare the input directory tree with the one copied out of the Deneb repo
    let mut output_root = dir.root.to_owned();
    output_root.pop();
    output_root.push("output");
    let comp = dir.compare(output_root.as_path());
    println!("Compare result: {:?}", comp);
    assert!(comp.is_ok());
}

#[test]
fn simple_fuse_hashmap_inout() {
    let tmp = TempDir::new("/tmp/deneb_test");
    assert!(tmp.is_ok());
    if let Ok(prefix) = tmp {
        let dt = make_test_dir_tree(prefix.path());
        assert!(dt.is_ok());
        if let Ok(dt) = dt {
            check_inout(dt, prefix.path());
        }

        // Explicit cleanup
        assert!(prefix.close().is_ok());
    }
}

// QuickCheck integration test
//
// Generate an random directory tree and use it to populate a Deneb repository.
// Copy all the files back out of the Deneb repository and compare with the originals.

// #[test]
// fn prop_inout_unchanged() {
//     fn inout_unchanged(dt: DirTree) -> bool {
//         dt.always_true()
//     }
//     QuickCheck::new().quickcheck(inout_unchanged as fn(DirTree) -> bool);
// }
