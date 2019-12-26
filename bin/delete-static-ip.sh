#!/bin/bash

SCRIPT_DIR=`dirname $0`
SHOW_USAGE=n

ADDR_NAME=
ADDR_PROJECT=
ADDR_PROJECT_DEF=$(gcloud config get-value project)
FORCE=n

while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

    if [[ "$1" == "--project" ]] ; then
        if [[ $# -gt 1 ]] ; then
            ADDR_PROJECT=$2
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

if [[ "${ADDR_PROJECT}" == "" ]] ; then
    ADDR_PROJECT=${ADDR_PROJECT_DEF}
fi

if [[ "${ADDR_PROJECT}" == "" ]] ; then
    echo "No project specified and none set as default."
    SHOW_USAGE=y
fi

if [[ $# == 1 ]] ; then
    ADDR_NAME=$1
else
    echo "Expected one name as argument."
    SHOW_USAGE=y
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
    echo "Usage: $0 [OPTIONS] <zone-name>"
    echo ""
    echo "Options:"
    echo "  --project <name>  The GCP project, default \"${ADDR_PROJECT_DEF}\"."
    echo "  -f                Force deleting the IP address; don't ask for confirmation."
    exit 1
fi

if [[ "${FORCE}" == "y" ]] ; then
    gcloud compute --project ${ADDR_PROJECT} adresses delete ${ADDR_NAME} --quiet --user-output-enabled=false
else
    ADDR_IP=$(gcloud compute --project ${ADDR_PROJECT} addresses list --filter="name=(${ADDR_NAME})" --format="value(address)")
    if [[ "${ADDR_DOMAIN}" != "" ]] ; then
        echo  -n "Delete static IP address \"${ADDR_NAME}\" in \"${ADDR_PROJECT}\" (\"${ADDR_IP}\")? [yN] " ; read yn
        if [[ "${yn}" == "y" || "${yn}" == "Y" ]] ; then
            gcloud compute --project ${ADDR_PROJECT} addresses delete ${ADDR_NAME} --quiet --user-output-enabled=false
        else
            echo "Not deleted."
            exit 1
        fi
    else
        echo "No such zone found."
        exit 1
    fi
fi
