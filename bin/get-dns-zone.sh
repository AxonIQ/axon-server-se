#!/bin/bash

SCRIPT_DIR=`dirname $0`
SHOW_USAGE=n

ZONE_NAME=
ZONE_PROJECT=
ZONE_PROJECT_DEF=$(gcloud config get-value project)
ZONE_VISIBILITY=private
ZONE_BY_DOMAIN=n

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
    elif [[ "$1" == "--by-domain" ]] ; then
        ZONE_BY_DOMAIN=y
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
    echo "Expected one argument."
    SHOW_USAGE=y
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
    echo "Usage: $0 [OPTIONS] <name>"
    echo ""
    echo "Options:"
    echo "  --project <name>  The GCP project, default \"${ZONE_PROJECT_DEF}\"."
    echo "  --public          Find a public DNS zone."
    echo "  --by-domain       The name to search is a domain."
    exit 1
fi

if [[ "${ZONE_BY_DOMAIN}" == "y" ]] ; then
    gcloud dns --project ${ZONE_PROJECT} managed-zones list --filter="dns_name=(${ZONE_NAME}.) AND visibility=(${ZONE_VISIBILITY})" --format="value(name)"
else
    gcloud dns --project ${ZONE_PROJECT} managed-zones list --filter="name=(${ZONE_NAME}) AND visibility=(${ZONE_VISIBILITY})" --format="value(dnsName)" | sed -e 's/.$//'
fi
