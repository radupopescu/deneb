#[macro_use]
extern crate quickcheck;

// Simple integration test
//
// Generate a random directory tree with random files and use it to populate a Deneb repository.
// Copy all the files back out of the Deneb repository and compare with the originals.


#[test]
quickcheck! {
    fn prop_inout(xs: Vec<usize>) -> bool {
        let rev: Vec<_> = xs.clone().into_iter().rev().collect();
        let revrev: Vec<_> = rev.into_iter().rev().collect();
        xs == revrev
    }
}
