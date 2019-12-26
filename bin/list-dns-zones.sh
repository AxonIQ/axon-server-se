#!/bin/bash

SCRIPT_DIR=`dirname $0`
SHOW_USAGE=n

ZONE_PROJECT=
ZONE_PROJECT_DEF=$(gcloud config get-value project)
ZONE_VISIBILITY=
FORMAT=

while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

    if [[ "$1" == "--project" ]] ; then
        if [[ $# -gt 1 ]] ; then
            ZONE_PROJECT=$2
            shift 2
        else
            echo "Missing project name after \"--project\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--public" ]] ; then
        ZONE_VISIBILITY=public
        shift
    elif [[ "$1" == "--private" ]] ; then
        ZONE_VISIBILITY=private
        shift
    elif [[ "$1" == "--format" ]] ; then
        if [[ $# -gt 1 ]] ; then
            FORMAT=$2
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

if [[ "${ZONE_PROJECT}" == "" ]] ; then
    ZONE_PROJECT=${ZONE_PROJECT_DEF}
fi

if [[ "${ZONE_PROJECT}" == "" ]] ; then
    echo "No project specified and none set as default."
    SHOW_USAGE=y
fi

if [[ $# == 1 ]] ; then
    ZONE_NAME=$1
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
    echo "Usage: $0 [OPTIONS] [<zone-name>]"
    echo ""
    echo "Options:"
    echo "  --project <name>  The GCP project, default \"${ZONE_PROJECT_DEF}\"."
    echo "  --public          List public DNS zones."
    echo "  --private         List private DNS zones."
    echo "  --format <spec>   Use a gcloud CLI format specifier."
    exit 1
fi

FILTER=
if [[ "${ZONE_NAME}" != "" ]] ; then
    FILTER="name=(${ZONE_NAME})"
fi
if [[ "${ZONE_VISIBILITY}" != "" ]] ; then
    if [[ "${FILTER}" != "" ]] ; then
        FILTER="${FILTER} AND "
    fi
    FILTER="${FILTER}visibility=(${ZONE_VISIBILITY})"
fi

if [[ "${FORMAT}" == "" ]] ; then
    gcloud dns --project ${ZONE_PROJECT} managed-zones list --filter="${FILTER}"
else
    gcloud dns --project ${ZONE_PROJECT} managed-zones list --filter="${FILTER}" --format="${FORMAT}"
fi
