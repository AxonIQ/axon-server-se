#!/bin/bash

SCRIPT_DIR=`dirname $0`

SHOW_USAGE=n

INST_NAME=
INST_TYPE=
INST_TYPE_DEF=n1-standard-2
INST_SETUP=
INST_DISK=
INST_DISK_DEF=10GB
INST_DISK_TYPE=
INST_DISK_TYPE_DEF=pd-standard
INST_LOG_DISK=
INST_EVT_DISK=
INST_IMAGE=
INST_IMAGE_PROJECT=
INST_IMAGE_PROJECT_DEF=axoniq-devops
INST_HOSTNAME=
INST_DOMAIN=
INST_DOMAIN_DEF=c.axoniq-devops.internal
INST_SUBNET=
INST_TAGS=
INST_TAGS_DEF=http-server
INST_ADDR=
INST_PROJECT=
INST_PROJECT_DEF=$(gcloud config get-value project)
INST_REGION=
INST_REGION_DEF=$(gcloud config get-value compute/region)
INST_ZONE=
INST_ZONE_DEF=$(gcloud config get-value compute/zone)

while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

    if [[ "$1" == "--project" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_PROJECT=$2
            shift 2
        else
            echo "Missing project name after \"--project\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--type" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_TYPE=$2
            shift 2
        else
            echo "Missing name after \"--type\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--setup" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_SETUP=$2
            shift 2
        else
            echo "Missing name after \"--setup\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--disk" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_DISK=$2
            shift 2
        else
            echo "Missing name after \"--disk\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--disk-type" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_DISK_TYPE=$2
            shift 2
        else
            echo "Missing name after \"--disk-type\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--log-disk" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_LOG_DISK=$2
            shift 2
        else
            echo "Missing name after \"--log-disk\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--evt-disk" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_EVT_DISK=$2
            shift 2
        else
            echo "Missing name after \"--evt-disk\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--image" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_IMAGE=$2
            shift 2
        else
            echo "Missing name after \"--image\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--image-project" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_IMAGE_PROJECT=$2
            shift 2
        else
            echo "Missing name after \"--image-project\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--hostname" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_HOSTNAME=$2
            shift 2
        else
            echo "Missing DNS hostname after \"--hostname\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--domain" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_DOMAIN=$2
            shift 2
        else
            echo "Missing DNS domain name after \"--domain\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--subnet" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_SUBNET=$2
            shift 2
        else
            echo "Missing name after \"--subnet\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--network-tags" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_TAGS=$2
            shift 2
        else
            echo "Missing tags after \"--network-tags\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--ip-address" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_ADDR=$2
            shift 2
        else
            echo "Missing name after \"--ip-address\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--region" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_REGION=$2
            shift 2
        else
            echo "Missing GCP Region name after \"--region\"."
            SHOW_USAGE=y
        fi
    elif [[ "$1" == "--zone" ]] ; then
        if [[ $# -gt 1 ]] ; then
            INST_ZONE=$2
            shift 2
        else
            echo "Missing GCP Zone name after \"--zone\"."
            SHOW_USAGE=y
        fi
    else
        echo "Unknown option \"$1\"."
        SHOW_USAGE=y
    fi

done

if [[ $# != 1 ]] ; then
    echo "Expected instance name as arguments"
    SHOW_USAGE=y
fi

INST_NAME=$1

if [[ "${INST_PROJECT}" == "" ]] ; then
    INST_PROJECT=${INST_PROJECT_DEF}
fi
if [[ "${INST_TYPE}" == "" ]] ; then
    INST_TYPE=${INST_TYPE_DEF}
fi
if [[ "${INST_DISK}" == "" ]] ; then
    INST_DISK=${INST_DISK_DEF}
fi
if [[ "${INST_DISK_TYPE}" == "" ]] ; then
    INST_DISK_TYPE=${INST_DISK_TYPE_DEF}
fi
if [[ "${INST_LOG_DISK}" == "" ]] ; then
    echo "WARNING: No log disk specified!"
fi
if [[ "${INST_EVT_DISK}" == "" ]] ; then
    echo "WARNING: No eventstore disk specified!"
fi
if [[ "${INST_IMAGE_PROJECT}" == "" ]] ; then
    INST_IMAGE_PROJECT=${INST_IMAGE_PROJECT_DEF}
fi
if [[ "${INST_HOSTNAME}" == "" ]] ; then
    INST_HOSTNAME=${INST_NAME}
fi
if [[ "${INST_DOMAIN}" == "" ]] ; then
    INST_DOMAIN=${INST_DOMAIN_DEF}
fi
if [[ "${INST_SUBNET}" == "" ]] ; then
    INST_SUBNET=${INST_PROJECT}-vpc
fi
if [[ "${INST_TAGS}" == "" ]] ; then
    INST_TAGS=${INST_TAGS_DEF}
fi
if [[ "${INST_ZONE}" == "" ]] ; then
    INST_ZONE=${INST_ZONE_DEF}
fi
if [[ "${INST_REGION}" == "" ]] ; then
    INST_REGION=${INST_REGION_DEF}
fi

if [[ "${INST_PROJECT}" == "" ]] then
    echo "No GCP project specified."
    SHOW_USAGE=y
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
    echo "Usage: $0 [OPTIONS] <instance-name>"
    exit 1
fi

INST_FQDN="${INST_HOSTNAME}.${INST_DOMAIN}"
DNS_ZONE=$(${SCRIPT_DIR}/get-dns-zone.sh --project ${INST_PROJECT} --by-domain ${INST_DOMAIN})
if [[ "${DNS_ZONE}" == "" ]] ; then
    echo "Creating DNS zone for domain \"${INST_DOMAIN}\"."
    DNS_ZONE=$(echo ${INST_DOMAIN} | tr '.' '-')-zone
    ${SCRIPT_DIR}/create-dns-zone.sh --project ${INST_PROJECT} --domain ${INST_DOMAIN} ${DNS_ZONE}
fi

if [[ "${INST_ADDR}" == "" ]] ; then
    INST_ADDR=$(${SCRIPT_DIR}/get-dns-record.sh --project ${INST_PROJECT} ${INST_NAME}-addr ${DNS_ZONE})
    if [[ "${INST_ADDR}" == "" ]] ; then
        INST_ADDR=$(${SCRIPT_DIR}/get-static-ip.sh --project ${INST_PROJECT} ${INST_NAME}-addr)
        if [[ "${INST_ADDR}" == "" ]] ; then
            echo "Reserving static IP address for \"${INST_FQDN}\"."
            INST_ADDR=$(${SCRIPT_DIR}/create-static-ip.sh --project ${INST_PROJECT} ${INST_NAME}-addr)
        fi
        echo "Creating DNS entry for \"${INST_NAME}\" in zone \"${DNS_ZONE}\"."
        ${SCRIPT_DIR}/create-dns-record.sh --project ${INST_PROJECT} ${INST_NAME} ${DNS_ZONE} ${INST_ADDR}
    fi
fi
echo "Server \"${INST_FQDN}\" has address \"${INST_ADDR}\"."

echo gcloud compute --project ${INST_PROJECT} instances create ${INST_NAME} --zone=${INST_ZONE} \
    --machine-type=${INST_TYPE} --metadata-from-file=startup-script=${INST_SETUP} \
    --boot-disk-size=${INST_DISK} --boot-disk-type=${INST_DISK_TYPE} --boot-disk-device-name=${INST_NAME} \
    --disk=name=${INST_LOG_DISK},device-name=${INST_LOG_DISK},mode=rw,boot=no \
    --disk=name=${INST_EVT_DISK},device-name=${INST_EVT_DISK},mode-rw,boot=no \
    --subnet=${INST_SUBNET} --tags=${INST_TAGS} --no-address \
    --private-network-ip=${INST_ADDR} --hostname=${INST_HOSTNAME}.${INST_DOMAIN}
