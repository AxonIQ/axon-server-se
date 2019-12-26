#!/bin/bash

SCRIPT_DIR=`dirname $0`
SHOW_USAGE=n

DNSREC_NAME=
DNSREC_PROJECT=
DNSREC_PROJECT_DEF=$(gcloud config get-value project)
DNSREC_ZONE=
DNSREC_NAME=
DNSREC_FORMAT=
DNSREC_FORMAT_DEF="value(rrdatas)"
DNSREC_VISIBILITY=private

while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

    if [[ "$1" == "--project" ]] ; then
        if [[ $# -gt 1 ]] ; then
            DNSREC_PROJECT=$2
            shift 2
        else
            echo "Missing project name after \"--project\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--format" ]] ; then
        if [[ $# -gt 1 ]] ; then
            DNSREC_FORMAT=$2
            shift 2
        else
            echo "Missing format specifier after \"--format\"."
            SHOW_USAGE=y
        fi
    else
        echo "Unknown option \"$1\"."
        SHOW_USAGE=y
    fi

done

if [[ "${DNSREC_PROJECT}" == "" ]] ; then
    DNSREC_PROJECT=${DNSREC_PROJECT_DEF}
fi
if [[ "${DNSREC_FORMAT}" == "" ]] ; then
    DNSREC_FORMAT=${DNSREC_FORMAT_DEF}
fi

if [[ "${DNSREC_PROJECT}" == "" ]] ; then
    echo "No project specified and none set as default."
    SHOW_USAGE=y
fi

if [[ $# == 2 ]] ; then
    DNSREC_NAME=$1
    DNSREC_ZONE=$2
else
    echo "Expected record name and zone name as arguments."
    SHOW_USAGE=y
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
    echo "Usage: $0 [OPTIONS] <record-name> <zone-name>"
    echo ""
    echo "Options:"
    echo "  --project <name>  The GCP project, default \"${DNSREC_PROJECT_DEF}\"."
    exit 1
fi

gcloud dns --project ${DNSREC_PROJECT} record-sets list --zone=${DNSREC_ZONE} --filter="type=(A) AND name~^${DNSREC_NAME}\..*" --format="${DNSREC_FORMAT}"
