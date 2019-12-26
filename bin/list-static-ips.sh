#!/bin/bash

SCRIPT_DIR=`dirname $0`
SHOW_USAGE=n

ADDR_PROJECT=
ADDR_PROJECT_DEF=$(gcloud config get-value project)
ADDR_TYPE=INTERNAL
ADDR_REGION=
ADDR_GLOBAL=
FORMAT=

while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

    if [[ "$1" == "--project" ]] ; then
        if [[ $# -gt 1 ]] ; then
            ADDR_PROJECT=$2
            shift 2
        else
            echo "Missing project name after \"--project\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--public" ]] ; then
        ADDR_TYPE=EXTERNAL
        shift
    elif [[ "$1" == "--global" ]] ; then
        ADDR_GLOBAL=--global
        shift
    elif [[ "$1" == "--format" ]] ; then
        if [[ $# -gt 1 ]] ; then
            FORMAT=$2
            shift 2
        else
            echo "Missing format specifier after \"--format\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--region" ]] ; then
        if [[ $# -gt 1 ]] ; then
            ADDR_REGION=$2
            shift 2
        else
            echo "Missing subnet name after \"--region\"."
            SHOW_USAGE=y
        fi
    else
        echo "Unknown option \"$1\"."
        SHOW_USAGE=y
    fi

done

if [[ "${ADDR_PROJECT}" == "" ]] ; then
    ADDR_PROJECT=${ADDR_PROJECT_DEF}
fi

if [[ "${ADDR_PROJECT}" == "" ]] ; then
    echo "No project specified and none set as default."
    SHOW_USAGE=y
fi

if [[ $# == 1 ]] ; then
    ADDR_NAME=$1
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
    echo "Usage: $0 [OPTIONS] [<zone-name>]"
    echo ""
    echo "Options:"
    echo "  --project <name>  The GCP project, default \"${ADDR_PROJECT_DEF}\"."
    echo "  --public          List public IP addresses rather than private ones."
    echo "  --global          List only global IP addresses."
    echo "  --region <name>   List only IP addresses for s specific region."
    echo "  --format <spec>   Use a gcloud CLI format specifier."
    exit 1
fi

FILTER=
if [[ "${ADDR_NAME}" != "" ]] ; then
    FILTER="name=(${ADDR_NAME})"
fi
if [[ "${ADDR_TYPE}" != "" ]] ; then
    if [[ "${FILTER}" != "" ]] ; then
        FILTER="${FILTER} AND "
    fi
    FILTER="${FILTER}addressType=(${ADDR_TYPE})"
fi
if [[ "${ADDR_REGION}" != "" ]] ; then
    if [[ "${FILTER}" != "" ]] ; then
        FILTER="${FILTER} AND "
    fi
    FILTER="${FILTER}region:(${ADDR_REGION})"
fi

if [[ "${FORMAT}" == "" ]] ; then
    gcloud compute --project ${ADDR_PROJECT} addresses list ${ADDR_GLOBAL} --filter="${FILTER}"
else
    gcloud compute --project ${ADDR_PROJECT} addresses list ${ADDR_GLOBAL} --filter="${FILTER}" --format="${FORMAT}"
fi
