#!/bin/bash

SCRIPT_DIR=`dirname $0`
SHOW_USAGE=n

DNSREC_PROJECT=
DNSREC_PROJECT_DEF=$(gcloud config get-value project)
DNSREC_NAME=
DNSREC_ZONE=
FORCE=n

while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

    if [[ "$1" == "--project" ]] ; then
        if [[ $# -gt 1 ]] ; then
            DNSREC_PROJECT=$2
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

if [[ "${DNSREC_PROJECT}" == "" ]] ; then
    DNSREC_PROJECT=${DNSREC_PROJECT_DEF}
fi

if [[ "${DNSREC_PROJECT}" == "" ]] ; then
    echo "No project specified and none set as default."
    SHOW_USAGE=y
fi

if [[ $# == 2 ]] ; then
    DNSREC_NAME=$1
    DNSREC_ZONE=$2
else
    echo "Expected record and zone names as argument."
    SHOW_USAGE=y
fi

DNSREC_DOMAIN=$(${SCRIPT_DIR}/get-dns-zone.sh ${DNSREC_ZONE})
if [[ "${DNSREC_DOMAIN}" == "" ]] ; then
    echo "Zone \"${DNSREC_ZONE}\" not found or no domain set."
    SHOW_USAGE=y
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
    echo "Usage: $0 [OPTIONS] <record-name> <zone-name>"
    echo ""
    echo "Options:"
    echo "  --project <name>  The GCP project, default \"${DNSREC_PROJECT_DEF}\"."
    echo "  -f                Force deleting the DNS zone; don't ask for confirmation."
    exit 1
fi

DNSREC_ADDR=$(${SCRIPT_DIR}/get-dns-record.sh --project ${DNSREC_PROJECT} ${DNSREC_NAME} ${DNSREC_ZONE})
if [[ "${DNSREC_ADDR}" == "" ]] ; then
    echo "No such record found."
    exit 1
fi

DNSREC_TTL=$(${SCRIPT_DIR}/get-dns-record.sh --project ${DNSREC_PROJECT} --format 'value(ttl)' ${DNSREC_NAME} ${DNSREC_ZONE})
if [[ "${FORCE}" == "y" ]] ; then
    gcloud dns --project ${DNSREC_PROJECT} record-sets transaction start --zone=${DNSREC_ZONE} --user-output-enabled=false
    gcloud dns --project ${DNSREC_PROJECT} record-sets transaction remove --zone=${DNSREC_ZONE} --name="${DNSREC_NAME}.${DNSREC_DOMAIN}" --type=A --ttl="${DNSREC_TTL}" "${DNSREC_ADDR}" --quiet --user-output-enabled=false
    gcloud dns --project ${DNSREC_PROJECT} record-sets transaction execute --zone=${DNSREC_ZONE} --user-output-enabled=false
else
    echo  -n "Delete record \"${DNSREC_NAME}\" in \"${DNSREC_PROJECT}\" for zone \"${DNSREC_ZONE}\"? [yN] " ; read yn
    if [[ "${yn}" == "y" || "${yn}" == "Y" ]] ; then
        gcloud dns --project ${DNSREC_PROJECT} record-sets transaction start --zone=${DNSREC_ZONE} --user-output-enabled=false
        gcloud dns --project ${DNSREC_PROJECT} record-sets transaction remove --zone=${DNSREC_ZONE} --name="${DNSREC_NAME}.${DNSREC_DOMAIN}" --type=A --ttl="${DNSREC_TTL}" "${DNSREC_ADDR}" --user-output-enabled=false
        gcloud dns --project ${DNSREC_PROJECT} record-sets transaction execute --zone=${DNSREC_ZONE} --user-output-enabled=false
    else
        echo "Not deleted."
        exit 1
    fi
fi
