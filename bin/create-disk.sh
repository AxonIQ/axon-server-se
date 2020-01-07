#!/bin/bash

SCRIPT_DIR=`dirname $0`
SHOW_USAGE=n

DISK_NAME=
DISK_PROJECT=
DISK_PROJECT_DEF=$(gcloud config get-value project)
DISK_ZONE=
DISK_ZONE_DEF=$(gcloud config get-value compute/zone)
DISK_SIZE=
DISK_SIZE_DEF=10GB
DISK_TYPE=
DISK_TYPE_DEF=pd-standard

while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

    if [[ "$1" == "--project" ]] ; then
        if [[ $# -gt 1 ]] ; then
            DISK_PROJECT=$2
            shift 2
        else
            echo "Missing project name after \"--project\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--zone" ]] ; then
        if [[ $# -gt 1 ]] ; then
            DISK_ZONE=$2
            shift 2
        else
            echo "Missing zone name after \"--zone\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--size" ]] ; then
        if [[ $# -gt 1 ]] ; then
            DISK_SIZE=$2
            shift 2
        else
            echo "Missing disk size after \"--size\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--type" ]] ; then
        if [[ $# -gt 1 ]] ; then
            DISK_TYPE=$2
            shift 2
        else
            echo "Missing type name after \"--type\"."
            SHOW_USAGE=y
        fi
    else
        echo "Unknown option \"$1\"."
        SHOW_USAGE=y
    fi

done

if [[ "${DISK_PROJECT}" == "" ]] ; then
    DISK_PROJECT=${DISK_PROJECT_DEF}
fi
if [[ "${DISK_ZONE}" == "" ]] ; then
    DISK_ZONE=${DISK_ZONE_DEF}
fi
if [[ "${DISK_SIZE}" == "" ]] ; then
    DISK_SIZE=${DISK_SIZE_DEF}
fi
if [[ "${DISK_TYPE}" == "" ]] ; then
    DISK_TYPE=${DISK_TYPE_DEF}
fi

if [[ "${DISK_PROJECT}" == "" ]] ; then
    echo "No project specified and none set as default."
    SHOW_USAGE=y
fi
if [[ "${DISK_ZONE}" == "" ]] ; then
    echo "No GCE zone specified and none set as default."
    SHOW_USAGE=y
fi

if [[ $# == 1 ]] ; then
    DISK_NAME=$1
else
    echo "Expected a disk name as argument."
    SHOW_USAGE=y
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
    echo "Usage: $0 [OPTIONS] <disk-name>"
    echo ""
    echo "Options:"
    echo "  --project <name>  The GCP project, default \"${DISK_PROJECT_DEF}\"."
    echo "  --zone <name>     The GCE zone for this disk, default \"${DISK_ZONE_DEF}\"."
    echo "  --size <size>     The size of the new disk, default \"${DISK_SIZE_DEF}\"."
    echo "  --type <name>     The type of this disk, default \"${DISK_TYPE_DEF}\"."
    exit 1
fi

gcloud compute --project ${DISK_PROJECT} disks create ${DISK_NAME} --zone=${DISK_ZONE} --type=${DISK_TYPE} --size=${DISK_SIZE} --user-output-enabled=false
