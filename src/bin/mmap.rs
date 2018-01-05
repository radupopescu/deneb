extern crate memmap;

use memmap::{Mmap, MmapMut, MmapOptions};

use std::fs::{File, OpenOptions};
use std::io::Write;

fn main() {
    // Read from existing memory-mapped file
    let f = File::open("README.md").unwrap();
    let file_mmap = unsafe { Mmap::map(&f) }.unwrap();
    assert_eq!(b"# Deneb", &file_mmap[0..7]);

    // Create a writable, anonymous memory-mapped file
    let mut anon_mmap = MmapOptions::new().len(4096).map_anon().unwrap();
    (&mut anon_mmap[..]).write_all(b"foo").unwrap();
    assert_eq!(b"foo\0\0", &anon_mmap[0..5]);

    // Create a new file, resize it and memory-map it to write to it through
    // multiple views
    let output_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open("/tmp/output.txt")
        .unwrap();
    output_file.set_len(20).unwrap();
    let mut output_map = unsafe { MmapMut::map_mut(&output_file) }.unwrap();

    let (mut view1, mut view2) = output_map.split_at_mut(7);
    view1.write_all(b"one").unwrap();
    view2.write_all(b"two").unwrap();
}
