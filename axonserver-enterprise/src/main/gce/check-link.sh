#!/bin/bash

CREATE_TARGET=n
if [[ $# == 3 && "$1" == "--create-target" ]] ; then
    CREATE_TARGET=y
    shift
fi
LINK=
TARGET=
if [[ $# == 2 ]] ; then
    LINK=$1
    TARGET=$2
else
    echo "Usage: $0 [--create-target] <symlink> <target-dir>"
    exit 1
fi

    if [ ${CREATE_TARGET} = y -a ! -d ${TARGET} ] ; then
        mkdir -p ${TARGET}
    fi
    if [ ! -L ${LINK} ] ; then
        ln -s ${TARGET} ${LINK}
    fi
