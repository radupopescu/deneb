#!/bin/sh

usage() { echo "Usage: $0 [-s] [-l off|error|warn|info|debug|trace] PREFIX_DIR" 1>&2; exit 1; }

do_sync=false
force_unmount=false
log_level=info

while getopts ":fsl:" o; do
    case "${o}" in
        f)
            force_unmount=true
            ;;
        s)
            do_sync=true
            ;;
        l)
            log_level=${OPTARG}
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done
shift $((OPTIND-1))

prefix=$@

if [ x"$prefix" = x"" ]; then
    usage
    exit 1
fi

args="-w $prefix/internal -m $prefix/mount -l $log_level"
if [ x"$do_sync" = x"true" ]; then
    args="-s $prefix/data $args"
fi
if [ x"$force_unmount" = x"true" ]; then
    args="-f $args"
fi

cargo run --release --bin deneb -- $args
