#!/bin/bash

SCRIPT_DIR=`dirname $0`
SHOW_USAGE=n

ZONE_NAME=
ZONE_PROJECT=
ZONE_PROJECT_DEF=$(gcloud config get-value project)
FORCE=n

while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

    if [[ "$1" == "--project" ]] ; then
        if [[ $# -gt 1 ]] ; then
            ZONE_PROJECT=$2
            shift 2
        else
            echo "Missing project name after \"--project\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "-f" ]] ; then
        FORCE=y
        shift
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
else
    echo "Expected one name as argument."
    SHOW_USAGE=y
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
    echo "Usage: $0 [OPTIONS] <zone-name>"
    echo ""
    echo "Options:"
    echo "  --project <name>  The GCP project, default \"${ZONE_PROJECT_DEF}\"."
    echo "  -f                Force deleting the DNS zone; don't ask for confirmation."
    exit 1
fi

if [[ "${FORCE}" == "y" ]] ; then
    gcloud dns --project ${ZONE_PROJECT} managed-zones delete ${ZONE_NAME} --quiet --user-output-enabled=false
else
    ZONE_DOMAIN=$(gcloud dns --project ${ZONE_PROJECT} managed-zones list --filter="name=(${ZONE_NAME})" --format="value(dnsName)")
    if [[ "${ZONE_DOMAIN}" != "" ]] ; then
        echo  -n "Delete zone \"${ZONE_NAME}\" in \"${ZONE_PROJECT}\" for domain \"${ZONE_DOMAIN}\"? [yN] " ; read yn
        if [[ "${yn}" == "y" || "${yn}" == "Y" ]] ; then
            gcloud dns --project ${ZONE_PROJECT} managed-zones delete ${ZONE_NAME}
        else
            echo "Not deleted."
            exit 1
        fi
    else
        echo "No such zone found."
        exit 1
    fi
fi
