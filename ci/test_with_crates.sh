#!/bin/sh

cd merkle
cargo test --verbose --jobs 1
cd ../
cargo test --verbose --jobs 1
