#![allow(dead_code)]

// use quickcheck::{Arbitrary, Gen};
use rust_sodium::crypto::hash::hash;

use std::fs::{File, create_dir_all, remove_dir_all};
use std::io::{Read, BufReader};
use std::io::Write;
use std::path::{Path, PathBuf};

use deneb::errors::*;

#[derive(Clone, Debug)]
pub enum DirEntry {
    File(String, Vec<u8>),
    Dir(String, Vec<DirEntry>),
}

/// Represents a directory tree which serves as input for the Deneb repository tests.
///
/// Implements the quickcheck::Arbitrary trait.
#[derive(Clone, Debug)]
pub struct DirTree {
    pub root: PathBuf,
    entries: Vec<DirEntry>,
}

impl DirTree {
    pub fn with_entries(root: PathBuf, entries: Vec<DirEntry>) -> DirTree {
        DirTree {
            root: root,
            entries: entries,
        }
    }

    pub fn show(&self) -> Result<()> {
        self.visit(&|dir: &Path, entry: &DirEntry| {
            if let DirEntry::File(ref name, ref contents) = *entry {
                if let Ok(body) = String::from_utf8(contents.to_vec()) {
                    println!("{:?} -> {:?}", dir.join(name), body);
                } else {
                    println!("{:?} -> {:?}", dir.join(name), contents);
                }
            }
            Ok(())
        })
    }

    pub fn create(&self) -> Result<()> {
        self.visit(&|dir: &Path, entry: &DirEntry| {
                        if let DirEntry::File(ref name, ref contents) = *entry {
                            create_dir_all(dir)?;
                            let mut f = File::create(dir.join(name))?;
                            f.write_all(contents)?;
                        }
                        Ok(())
                    })
    }

    pub fn compare(&self, other: &Path) -> Result<()> {
        self.visit(&|dir: &Path, entry: &DirEntry| {
            if let DirEntry::File(ref name, ref _contents) = *entry {
                let input_file_name = dir.join(name);
                let relative_path = input_file_name.strip_prefix(self.root.as_path())?;
                let output_file_name = other.to_owned().join(relative_path);
                if !compare_files(input_file_name.as_path(), output_file_name.as_path()) {
                    bail!("File {:?} modified.", name);
                }
            }
            Ok(())
        })
    }

    pub fn clean_up(&self) -> Result<()> {
        Ok(remove_dir_all(&self.root)?)
    }

    fn visit<V>(&self, action: V) -> Result<()>
        where V: Fn(&Path, &DirEntry) -> Result<()> + Copy
    {
        self.visit_rec(&self.root, &self.entries, action)
    }

    fn visit_rec<V>(&self, dir: &Path, entries: &Vec<DirEntry>, action: V) -> Result<()>
        where V: Fn(&Path, &DirEntry) -> Result<()> + Copy
    {
        for entry in entries.iter() {
            action(dir, &entry)?;
            if let DirEntry::Dir(ref name, ref children) = *entry {
                self.visit_rec(&dir.join(name), children, action)?;
            }
        }
        Ok(())
    }

    fn new() -> DirTree {
        DirTree {
            root: PathBuf::from(""),
            entries: Vec::new(),
        }
    }
}

fn compare_files(fn1: &Path, fn2: &Path) -> bool {
    match (File::open(fn1), File::open(fn2)) {
        (Ok(f1), Ok(f2)) => {
            let mut buffer1 = Vec::new();
            let mut buffer2 = Vec::new();
            let _ = BufReader::new(f1).read_to_end(&mut buffer1);
            let _ = BufReader::new(f2).read_to_end(&mut buffer2);
            let digest1 = hash(buffer1.as_ref());
            let digest2 = hash(buffer2.as_ref());
            digest1 == digest2
        }
        _ => false
    }
}

// impl Arbitrary for DirTree {
//     fn arbitrary<G: Gen>(g: &mut G) -> Self {
//         DirTree::new()
//     }

//     fn shrink(&self) -> Box<Iterator<Item=DirTree>> {
//     }
// }
