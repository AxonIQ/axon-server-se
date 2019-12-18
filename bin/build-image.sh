#!/bin/bash

SHOW_USAGE=n

VERSION=
MVN_MODULE=
IMG_VERSION=
IMG_FAMILY=
IMG_FAMILY_DEF=axonserver
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

  if [[ "$1" == "--project" ]] ; then
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
  elif [[ "$1" == "--mvn-module" ]] ; then
    if [[ $# -gt 1 ]] ; then
      MVN_MODULE=$2
      shift 2
    else
      echo "Missing module name after \"--mvn-module\"."
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
  elif [[ "$1" == "--public-ip" ]] ; then
    NO_PUBLIC_IP=false
    shift
  else
    echo "Unknown option \"$1\"."
    SHOW_USAGE=y
  fi

done

if [[ $# == 1 ]] ; then
  VERSION=$1
else
  echo "Missing project version."
  SHOW_USAGE=y
fi

if [[ "${IMG_VERSION}" == "" ]] ; then
  IMG_VERSION=`echo ${VERSION} | tr '.' '-' | tr '[A-Z]' '[a-z]'`
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
if [[ "${MVN_MODULE}" != "" ]] ; then
  MVN_MODULE="${MVN_MODULE}/"
fi

if [[ "${IMG_FAMILY}" == "" ]] ; then
  echo "No Image family set."
  SHOW_USAGE=y
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
    echo "Usage: $0 [OPTIONS] <version>"
    echo ""
    echo "Options:"
    echo "  --project <gce-project>   The GCE project to create the image in, default \"${PROJECT_DEF}\"."
    echo "  --zone <gce-zone>         The GCE zone to create the image (and run the instance to build it from), default \"${ZONE_DEF}\"."
    echo "  --network <gce-network>   The GCE network to use, default \"<project-name>-vpc\"."
    echo "  --subnet <gce-subnet>     The GCE subnet to use, defaults to the same name is the network."
    echo "  --mvn-module <name>       The Maven module containing the sources and JAR. Default is the root/parent project."
    echo "  --img-version <version>   The version suffix to append to the image name. Default is the project version in lowercase."
    echo "  --img-family <name>       The name for the image-family. Default is \"${IMG_FAMILY_DEF}\"."
    echo "  --img-name <name>         The name for the image. Default is the family name, a dash, and the version."
    echo "  --img-user <username>     The username for the application owner. Default is \"${IMG_USER_DEF}\"."
    echo "  --public-ip               Use a public IP during build."
    exit 1
fi

LABEL=`echo ${VERSION} | tr '.' '-' | tr '[A-Z]' '[a-z]'`
cat > target/application-image.json <<EOF
{
  "builders": [
    {
      "type": "googlecompute",
      "project_id": "${PROJECT}",
      "source_image_family": "centos-7",
      "source_image_project_id": "gce-uefi-images",
      "zone": "${ZONE}",
      "network": "${NETWORK}",
      "subnetwork": "${SUBNET}",
      "omit_external_ip": ${NO_PUBLIC_IP},
      "use_internal_ip": ${NO_PUBLIC_IP},
      "disk_size": "10",
      "image_name": "${IMG_NAME}",
      "image_family": "${IMG_FAMILY}",
      "image_labels": {
        "version": "${LABEL}"
      },
      "ssh_username": "axoniq"
    }
  ],
  "provisioners": [
    {
        "type": "file",
        "source": "target/axoniq-${IMG_FAMILY}.conf",
        "destination": "/tmp/axoniq-${IMG_FAMILY}.conf"
    },
    {
        "type": "file",
        "source": "${MVN_MODULE}target/${IMG_FAMILY}-${VERSION}-exec.jar",
        "destination": "/tmp/${IMG_FAMILY}.jar"
    },
    {
        "type": "file",
        "source": "${MVN_MODULE}src/main/gce/axonserver.yml",
        "destination": "/tmp/axonserver.yml"
    },
    {
        "type": "file",
        "source": "target/startup.sh",
        "destination": "/tmp/startup.sh"
    },
    {
        "type": "file",
        "source": "${MVN_MODULE}src/main/gce/mount-disk.sh",
        "destination": "/tmp/mount-disk.sh"
    },
    {
      "type": "shell",
      "inline": [ "sudo yum -y update",
                  "sudo yum -y install java-11-openjdk-headless dejavu-sans-fonts urw-fonts wget curl",
                  "sudo adduser -d /opt/${IMG_USER} -U ${IMG_USER}",
                  "curl -sSO https://dl.google.com/cloudagents/install-logging-agent.sh",
                  "sudo bash ./install-logging-agent.sh",
                  "sudo cp /tmp/axonserver.yml /opt/${IMG_USER}/",
                  "sudo cp /tmp/${IMG_FAMILY}.jar /opt/${IMG_USER}/",
                  "sudo cp /tmp/startup.sh /opt/${IMG_USER}/",
                  "sudo cp /tmp/mount-disk.sh /opt/${IMG_USER}/",
                  "sudo chown -R ${IMG_USER}:${IMG_USER} /opt/${IMG_USER}",
                  "sudo mkdir -p /etc/google-fluentd/config.d",
                  "sudo cp /tmp/axoniq-${IMG_FAMILY}.conf /etc/google-fluentd/config.d/",
                  "sudo service google-fluentd restart" ]
    }
  ]
}
EOF

packer build -force -color=false target/application-image.json
