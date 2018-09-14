#!/bin/sh

usage() { echo "Usage: $0 [-s] [-l off|error|warn|info|debug|trace] PREFIX_DIR" 1>&2; exit 1; }

do_sync=false
force_unmount=false
background=false
log_level=info

while getopts ":fbsl:" o; do
    case "${o}" in
        f)
            force_unmount=true
            ;;
        b)
            background=true
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

mkdir -p $prefix/{internal,mount,data}

args="-w $prefix/internal -m $prefix/mount -l $log_level"
if [ x"$background" = x"false" ]; then
    args="--foreground $args"
fi
if [ x"$do_sync" = x"true" ]; then
    args="-s $prefix/data $args"
fi
if [ x"$force_unmount" = x"true" ]; then
    args="-f $args"
fi

cargo run --release --bin deneb -- $args
