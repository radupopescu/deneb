extern crate memmap;

use memmap::{Mmap, Protection};

use std::fs::OpenOptions;
use std::io::Write;

fn main() {
    // Read from existing memory-mapped file
    let file_mmap = Mmap::open_path("README.md", Protection::Read).unwrap();
    let bytes: &[u8] = unsafe { file_mmap.as_slice() };
    assert_eq!(b"# Deneb", &bytes[0..7]);

    // Create a writable, anonymous memory-mapped file
    let mut anon_mmap = Mmap::anonymous(4096, Protection::ReadWrite).unwrap();
    unsafe { anon_mmap.as_mut_slice() }
        .write(b"foo")
        .unwrap();
    assert_eq!(b"foo\0\0", unsafe { &anon_mmap.as_slice()[0..5] });

    // Create a new file, resize it and memory-map it to write to it through
    // multiple views
    let output_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open("/tmp/output.txt").unwrap();
    let _ = output_file.set_len(20).unwrap();
    let output_map = Mmap::open(&output_file, Protection::ReadWrite).unwrap();

    let mut view1 = output_map.into_view();
    let mut view2 = unsafe { view1.clone() };
    let mut view3 = unsafe { view2.clone() };

    let _ = view1.restrict(2, 5).unwrap();
    let mut buffer1 = unsafe { view1.as_mut_slice() };
    buffer1.write(b"one").unwrap();

    let _ = view2.restrict(7, 5).unwrap();
    let mut buffer2 = unsafe { view2.as_mut_slice() };
    buffer2.write(b"two").unwrap();

    let mut buffer3 = unsafe { view3.as_mut_slice() };
    (&mut buffer3[12..]).write(b"three").unwrap();
}
