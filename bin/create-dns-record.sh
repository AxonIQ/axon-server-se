#!/bin/bash

SCRIPT_DIR=`dirname $0`
SHOW_USAGE=n

DNSREC_PROJECT=
DNSREC_PROJECT_DEF=$(gcloud config get-value project)
DNSREC_NAME=
DNSREC_ZONE=
DNSREC_ADDR=
DNSREC_TTL=
DNSREC_TTL_DEF=14400

while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

    if [[ "$1" == "--project" ]] ; then
        if [[ $# -gt 1 ]] ; then
            DNSREC_PROJECT=$2
            shift 2
        else
            echo "Missing project name after \"--project\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--ttl" ]] ; then
        if [[ $# -gt 1 ]] ; then
            DNSREC_TTL=$2
            shift 2
        else
            echo "Missing number of seconds after \"--ttl\"."
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
if [[ "${DNSREC_TTL}" == "" ]] ; then
    DNSREC_TTL=${DNSREC_TTL_DEF}
fi

if [[ "${DNSREC_PROJECT}" == "" ]] ; then
    echo "No project specified and none set as default."
    SHOW_USAGE=y
fi

if [[ $# == 3 ]] ; then
    DNSREC_NAME=$1
    DNSREC_ZONE=$2
    DNSREC_ADDR=$3
else
    echo "Expected name, zone, and IPv4 address as arguments."
    SHOW_USAGE=y
fi

DNSREC_DOMAIN=$(${SCRIPT_DIR}/get-dns-zone.sh ${DNSREC_ZONE})
if [[ "${DNSREC_DOMAIN}" == "" ]] ; then
    echo "Zone \"${DNSREC_ZONE}\" not found or no domain set."
    SHOW_USAGE=y
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
    echo "Usage: $0 [OPTIONS] <record-name> <zone-name> <ipv4-address>"
    echo ""
    echo "Options:"
    echo "  --project <name>  The GCP project, default \"${DNSREC_PROJECT_DEF}\"."
    echo "  --ttl <seconds>   The Time-To-Live for this record, default ${DNSREC_TTL_DEF}."
    exit 1
fi

gcloud dns --project ${DNSREC_PROJECT} record-sets transaction start --zone=${DNSREC_ZONE} --user-output-enabled=false
gcloud dns --project ${DNSREC_PROJECT} record-sets transaction add "${DNSREC_ADDR}" --name="${DNSREC_NAME}.${DNSREC_DOMAIN}" --type=A --ttl=${DNSREC_TTL} --zone=${DNSREC_ZONE} --user-output-enabled=false
gcloud dns --project ${DNSREC_PROJECT} record-sets transaction execute --zone=${DNSREC_ZONE} --user-output-enabled=false
