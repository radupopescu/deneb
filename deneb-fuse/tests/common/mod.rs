#![allow(dead_code)]

use quickcheck::{Arbitrary, Gen};
use rand::{thread_rng, Rng};
use uuid::Uuid;

use std::fs::{create_dir_all, remove_dir_all, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use deneb_core::errors::DenebResult;
use deneb_core::util::atomic_write;
use deneb_core::cas::hash;

#[derive(Clone, Debug)]
pub enum DirEntry {
    File(String, Vec<u8>),
    Dir(String, Vec<DirEntry>),
}

impl DirEntry {
    fn arbitrary_rec<G: Gen>(g: &mut G, current_depth: usize) -> DirEntry {
        let max_size = g.size();
        let name = Uuid::new_v4().simple().to_string();
        match g.gen_range(0, 2) {
            0 => {
                let file_size = g.gen_range(0, max_size * 100);
                let mut contents = vec![0 as u8; file_size];
                thread_rng().fill_bytes(contents.as_mut());
                DirEntry::File(name, contents)
            }
            1 => {
                let mut children = Vec::new();
                if current_depth < max_size {
                    for _i in 0..g.gen_range(0, max_size) {
                        children.push(DirEntry::arbitrary_rec(g, current_depth + 1));
                    }
                }
                DirEntry::Dir(name, children)
            }
            _ => panic!("Not supposed to be here"),
        }
    }
}

impl Arbitrary for DirEntry {
    fn arbitrary<G: Gen>(g: &mut G) -> DirEntry {
        DirEntry::arbitrary_rec(g, 0)
    }

    // fn shrink(&self) -> Box<Iterator<Item=DirTree>> {
    // }
}

/// Represents a directory tree which serves as input for the Deneb repository tests.
///
/// Implements the `quickcheck::Arbitrary` trait.
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

    pub fn show(&self) -> DenebResult<()> {
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

    pub fn create(&self) -> DenebResult<()> {
        self.visit(&|dir: &Path, entry: &DirEntry| {
            if let DirEntry::File(ref name, ref contents) = *entry {
                create_dir_all(dir)?;
                atomic_write(dir.join(name).as_path(), contents)?;
            }
            Ok(())
        })
    }

    pub fn compare(&self, other: &Path) -> DenebResult<()> {
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

    pub fn clean_up(&self) -> DenebResult<()> {
        Ok(remove_dir_all(&self.root)?)
    }

    fn visit<V>(&self, action: V) -> DenebResult<()>
    where
        V: Fn(&Path, &DirEntry) -> DenebResult<()> + Copy,
    {
        self.visit_rec(&self.root, &self.entries, action)
    }

    fn visit_rec<V>(&self, dir: &Path, entries: &[DirEntry], action: V) -> DenebResult<()>
    where
        V: Fn(&Path, &DirEntry) -> DenebResult<()> + Copy,
    {
        for entry in entries.iter() {
            action(dir, entry)?;
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
        (Ok(_f1), Err(_e2)) => {
            println!("Error opening new file {:?}", fn2);
            false
        }
        (Err(_e1), Ok(_f2)) => {
            println!("Error opening old file {:?}", fn1);
            false
        }
        _ => false,
    }
}

impl Arbitrary for DirTree {
    fn arbitrary<G: Gen>(g: &mut G) -> DirTree {
        let max_size = g.size();
        let num_items = g.gen_range(0, max_size);
        let mut items = Vec::new();
        for _i in 0..num_items {
            items.push(<DirEntry as Arbitrary>::arbitrary(g));
        }
        DirTree::with_entries(PathBuf::from(""), items)
    }

    // fn shrink(&self) -> Box<Iterator<Item=DirTree>> {
    // }
}
