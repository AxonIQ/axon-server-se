#!/bin/bash

SCRIPT_DIR=$(dirname $0)

SHOW_USAGE=n

TARGET=
TARGET_DEF=target/packer
SERVER_VERSION=
CLI_VERSION=
DISK_IMAGE_FAMILY=
DISK_IMAGE_FAMILY_DEF=centos-7
DISK_IMAGE_PROJECT=
DISK_IMAGE_PROJECT_DEF=gce-uefi-images
DISK_SIZE=
DISK_SIZE_DEF=20
IMG_VERSION=
IMG_FAMILY=
IMG_FAMILY_DEF=axonserver-enterprise
IMG_NAME=
IMG_USER=
IMG_USER_DEF=axonserver
PROJECT=
PROJECT_DEF=$(gcloud config get-value project)
ZONE=
ZONE_DEF=$(gcloud config get-value compute/zone)
NETWORK=
SUBNET=
NO_PUBLIC_IP=true

while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

  if [[ "$1" == "--target" ]] ; then
    if [[ $# -gt 1 ]] ; then
      TARGET=$2
      shift 2
    else
      echo "Missing directory name after \"--target\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--project" ]] ; then
    if [[ $# -gt 1 ]] ; then
      PROJECT=$2
      shift 2
    else
      echo "Missing project name after \"--project\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--zone" ]] ; then
    if [[ $# -gt 1 ]] ; then
      ZONE=$2
      shift 2
    else
      echo "Missing zone name after \"--zone\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--network" ]] ; then
    if [[ $# -gt 1 ]] ; then
      NETWORK=$2
      shift 2
    else
      echo "Missing network name after \"--network\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--subnet" ]] ; then
    if [[ $# -gt 1 ]] ; then
      SUBNET=$2
      shift 2
    else
      echo "Missing subnet name after \"--subnet\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--img-version" ]] ; then
    if [[ $# -gt 1 ]] ; then
      IMG_VERSION=$2
      shift 2
    else
      echo "Missing version name after \"--img-version\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--img-family" ]] ; then
    if [[ $# -gt 1 ]] ; then
      IMG_FAMILY=$2
      shift 2
    else
      echo "Missing image family name after \"--img-family\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--img-name" ]] ; then
    if [[ $# -gt 1 ]] ; then
      IMG_NAME=$2
      shift 2
    else
      echo "Missing image name after \"--img-name\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--img-user" ]] ; then
    if [[ $# -gt 1 ]] ; then
      IMG_USER=$2
      shift 2
    else
      echo "Missing username after \"--img-user\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--disk-img-family" ]] ; then
    if [[ $# -gt 1 ]] ; then
      DISK_IMAGE_FAMILY=$2
      shift 2
    else
      echo "Missing image family name after \"--disk-img-family\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--disk-img-project" ]] ; then
    if [[ $# -gt 1 ]] ; then
      DISK_IMAGE_PROJECT=$2
      shift 2
    else
      echo "Missing project family name after \"--disk-img-project\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--disk-size" ]] ; then
    if [[ $# -gt 1 ]] ; then
      DISK_SIZE=$2
      shift 2
    else
      echo "Missing image name after \"--disk-size\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--cli-version" ]] ; then
    if [[ $# -gt 1 ]] ; then
      CLI_VERSION=$2
      shift 2
    else
      echo "Missing version after \"--cli-version\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--public-ip" ]] ; then
    NO_PUBLIC_IP=false
    shift
  else
    echo "Unknown option \"$1\"."
    SHOW_USAGE=y
  fi

done

if [[ $# == 1 ]] ; then
  SERVER_VERSION=$1
else
  echo "Missing project version."
  SHOW_USAGE=y
fi

if [[ "${TARGET}" == "" ]] ; then
  TARGET=${TARGET_DEF}
fi
if [[ "${IMG_VERSION}" == "" ]] ; then
  IMG_VERSION=`echo ${SERVER_VERSION} | tr '.' '-' | tr '[A-Z]' '[a-z]'`
fi
if [[ "${IMG_FAMILY}" == "" ]] ; then
  IMG_FAMILY=${IMG_FAMILY_DEF}
fi
if [[ "${IMG_NAME}" == "" ]] ; then
  IMG_NAME=${IMG_FAMILY}-${IMG_VERSION}
fi
if [[ "${IMG_USER}" == "" ]] ; then
  IMG_USER=${IMG_USER_DEF}
fi
if [[ "${DISK_IMAGE_FAMILY}" == "" ]] ; then
  DISK_IMAGE_FAMILY=${DISK_IMAGE_FAMILY_DEF}
fi
if [[ "${DISK_IMAGE_PROJECT}" == "" ]] ; then
  DISK_IMAGE_PROJECT=${DISK_IMAGE_PROJECT_DEF}
fi
if [[ "${DISK_SIZE}" == "" ]] ; then
  DISK_SIZE=${DISK_SIZE_DEF}
fi
if [[ "${PROJECT}" == "" ]] ; then
  PROJECT=${PROJECT_DEF}
fi
if [[ "${ZONE}" == "" ]] ; then
  ZONE=${ZONE_DEF}
fi
if [[ "${NETWORK}" == "" ]] ; then
  NETWORK=${PROJECT}-vpc
fi
if [[ "${SUBNET}" == "" ]] ; then
  SUBNET=${NETWORK}
fi

if [[ "${CLI_VERSION}" == "" ]] ; then
    echo "WARNING: Assuming CLI has version \"${SERVER_VERSION}\"."
    CLI_VERSION=${SERVER_VERSION}
fi
if [[ "${IMG_FAMILY}" == "" ]] ; then
  echo "No Image family set."
  SHOW_USAGE=y
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
    echo "Usage: $0 [OPTIONS] <version>"
    echo ""
    echo "Options:"
    echo "  --target <dir-name>       The name for the target directory. Default is \"${TARGET_DEF}\"."
    echo "  --project <gce-project>   The GCE project to create the image in, default \"${PROJECT_DEF}\"."
    echo "  --zone <gce-zone>         The GCE zone to create the image (and run the instance to build it from), default \"${ZONE_DEF}\"."
    echo "  --network <gce-network>   The GCE network to use, default \"<project-name>-vpc\"."
    echo "  --subnet <gce-subnet>     The GCE subnet to use, defaults to the same name is the network."
    echo "  --img-version <version>   The version suffix to append to the image name. Default is the project version in lowercase."
    echo "  --img-family <name>       The name for the image-family. Default is \"${IMG_FAMILY_DEF}\"."
    echo "  --img-name <name>         The name for the image. Default is the family name, a dash, and the version."
    echo "  --img-user <username>     The username for the application owner. Default is \"${IMG_USER_DEF}\"."
    echo "  --disk-img-family <name>  The name of the base disk image's family. Default is \"${DISK_IMAGE_FAMILY_DEF}\"."
    echo "  --disk-img-project <name> The name of the project for the base disk image. Default is \"${DISK_IMAGE_PROJECT_DEF}\"."
    echo "  --disk-size <size-in-gb>  The size of the base disk image in GiB. Default is \"${DISK_SIZE_DEF}\"."
    echo "  --cli-version <version>   The version of the Axon Server CLI. Default is to use the Axon Server EE version."
    echo "  --public-ip               Use a public IP during build."
    exit 1
fi

mkdir -p target
if ! ${SCRIPT_DIR}/prep-files.sh --target ${TARGET} --cli-version ${CLI_VERSION} ${SERVER_VERSION} ; then
    echo "Failed to prepare files."
    exit 1
fi

LABEL=`echo ${SERVER_VERSION} | tr '.' '-' | tr '[A-Z]' '[a-z]'`

cat > target/application-image.json <<EOF
{
  "builders": [
    {
      "type": "googlecompute",
      "project_id": "${PROJECT}",
      "source_image_family": "${DISK_IMAGE_FAMILY}",
      "source_image_project_id": "${DISK_IMAGE_PROJECT}",
      "disk_size": "${DISK_SIZE}",
      "zone": "${ZONE}",
      "network": "${NETWORK}",
      "subnetwork": "${SUBNET}",
      "omit_external_ip": ${NO_PUBLIC_IP},
      "use_internal_ip": ${NO_PUBLIC_IP},
      "image_name": "${IMG_NAME}",
      "image_family": "${IMG_FAMILY}",
      "image_labels": {
        "kind": "axonserver-enterprise",
        "version": "${LABEL}"
      },
      "ssh_username": "axoniq"
    }
  ],
  "provisioners": [
    {
        "type": "shell",
        "inline": [ "mkdir /tmp/${LABEL}"]
    },
    {
        "type": "file",
        "source": "${TARGET}/",
        "destination": "/tmp/${LABEL}/"
    },
    {
      "type": "shell",
      "inline": [ "sudo yum -y update",
                  "sudo yum -y install java-11-openjdk-headless dejavu-sans-fonts urw-fonts wget curl jq",
                  "sudo bash -c 'echo LANG=en_US.utf-8 >> /etc/environment'",
                  "sudo bash -c 'echo LC_ALL=en_US.utf-8 >> /etc/environment'",
                  "sudo chmod 755 /tmp/${LABEL}/setup-user.sh",
                  "sudo /tmp/${LABEL}/setup-user.sh axonserver /tmp/${LABEL}",
                  "sudo rm -rf /tmp/${LABEL}" ]
    }
  ]
}
EOF

packer build -force -color=false target/application-image.json
