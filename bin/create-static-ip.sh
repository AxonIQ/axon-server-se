#!/bin/bash

SCRIPT_DIR=`dirname $0`
SHOW_USAGE=n

ADDR_NAME=
ADDR_PROJECT=
ADDR_PROJECT_DEF=$(gcloud config get-value project)
ADDR_GLOBAL=n
ADDR_NETWORK=
ADDR_SUBNET=
ADDR_REGION=
ADDR_REGION_DEF=$(gcloud config get-value compute/region)
ADDR_PREFIX=
ADDR_PREFIX_DEF=20

while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

    if [[ "$1" == "--project" ]] ; then
        if [[ $# -gt 1 ]] ; then
            ADDR_PROJECT=$2
            shift 2
        else
            echo "Missing project name after \"--project\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--global" ]] ; then
        ADDR_GLOBAL=y
        shift
    elif [[ "$1" == "--network" ]] ; then
        if [[ $# -gt 1 ]] ; then
            ADDR_NETWORK=$2
            shift 2
        else
            echo "Missing network name after \"--network\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--subnet" ]] ; then
        if [[ $# -gt 1 ]] ; then
            ADDR_SUBNET=$2
            shift 2
        else
            echo "Missing subnet name after \"--subnet\"."
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
    elif [[ "$1" == "--prefix-length" ]] ; then
        if [[ $# -gt 1 ]] ; then
            ADDR_PREFIX=$2
            shift 2
        else
            echo "Missing prefix length after \"--prefix-length\"."
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
if [[ "${ADDR_GLOBAL}" == "y" ]] ; then
    if [[ "${ADDR_NETWORK}" == "" ]] ; then
        ADDR_NETWORK=${ADDR_PROJECT}-vpc
    fi
else
    if [[ "${ADDR_SUBNET}" == "" ]] ; then
        ADDR_SUBNET=${ADDR_PROJECT}-vpc
    fi
    if [[ "${ADDR_REGION}" == "" ]] ; then
        ADDR_REGION=${ADDR_REGION_DEF}
    fi
fi
if [[ "${ADDR_PREFIX}" == "" ]] ; then
    ADDR_PREFIX=${ADDR_PREFIX_DEF}
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
    echo "Usage: $0 [OPTIONS] <address-name>"
    echo ""
    echo "Options:"
    echo "  --project <name>  The GCP project, default \"${ADDR_PROJECT_DEF}\"."
    echo "  --global          Create a global static internal IP."
    echo "  --network <name>  For global addresses, the name of the VPC, default \"<project-name>-vpc\"."
    echo "  --prefix <name>   For global addresses, the IPv4 address prefix-length, default \"${ADDR_PREFIX}\"."
    echo "  --subnet <name>   For regional addresses, the name of the subnet, default \"<project-name>-vpc\"."
    echo "  --region <name>   For regional addresses, the name of the region, default \"${ADDR_REGION_DEF}\"."
    exit 1
fi

if [[ "${ADDR_GLOBAL}" == "y" ]] ; then
    gcloud compute --project ${ADDR_PROJECT} addresses create ${ADDR_NAME} --global --network ${ADDR_NETWORK} --prefix-length ${ADDR_PREFIX} --user-output-enabled=false
else
    gcloud compute --project ${ADDR_PROJECT} addresses create ${ADDR_NAME} --region ${ADDR_REGION} --subnet ${ADDR_SUBNET} --user-output-enabled=false
fi
gcloud compute --project ${ADDR_PROJECT} addresses list --filter="name=(${ADDR_NAME})" --format="value(address)"