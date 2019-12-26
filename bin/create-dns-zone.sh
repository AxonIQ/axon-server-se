#!/bin/bash

SCRIPT_DIR=`dirname $0`
SHOW_USAGE=n

ZONE_NAME=
ZONE_PROJECT=
ZONE_PROJECT_DEF=$(gcloud config get-value project)
ZONE_PUBLIC=n
ZONE_NETWORK=
ZONE_DOMAIN=

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
        ZONE_PUBLIC=y
        shift
    elif [[ "$1" == "--network" ]] ; then
        if [[ $# -gt 1 ]] ; then
            ZONE_NETWORK=$2
            shift 2
        else
            echo "Missing network name after \"--network\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--domain" ]] ; then
        if [[ $# -gt 1 ]] ; then
            ZONE_DOMAIN=$2
            shift 2
        else
            echo "Missing DNS name after \"--domain\"."
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
if [[ "${ZONE_PUBLIC}" == "n" ]] ; then
    if [[ "${ZONE_NETWORK}" == "" ]] ; then
        ZONE_NETWORK=${ZONE_PROJECT}-vpc
    fi
fi

if [[ "${ZONE_PROJECT}" == "" ]] ; then
    echo "No project specified and none set as default."
    SHOW_USAGE=y
fi
if [[ "${ZONE_DOMAIN}" == "" ]] ; then
    echo "No DNS domain specified for this zone."
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
    echo "  --public          Create a public DNS zone."
    echo "  --network <name>  For private zones, the name of the VPC, default \"<project-name>-vpc\"."
    echo "  --domain <name>   The DNS domain this zone controls."
    exit 1
fi

if [[ "${ZONE_PUBLIC}" == "y" ]] ; then
    gcloud dns --project ${ZONE_PROJECT} managed-zones create ${ZONE_NAME} --description= --dns-name=${ZONE_DOMAIN} --visibility=public --user-output-enabled=false
else
    gcloud dns --project ${ZONE_PROJECT} managed-zones create ${ZONE_NAME} --description= --dns-name=${ZONE_DOMAIN} --networks=${ZONE_NETWORK} --visibility=private --user-output-enabled=false
fi
