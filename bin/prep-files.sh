#!/bin/bash

SHOW_USAGE=n

VERSION=
IMG_VERSION=
IMG_FAMILY=
IMG_FAMILY_DEF=axonserver
IMG_USER=
IMG_USER_DEF=axonserver

while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

  if [[ "$1" == "--img-family" ]] ; then
    if [[ $# -gt 1 ]] ; then
      IMG_FAMILY=$2
      shift 2
    else
      echo "Missing image family name after \"--img-family\"."
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
  else
    echo "Unknown option \"$1\"."
    SHOW_USAGE=y
  fi

done

if [[ $# == 1 ]] ; then
  IMG_VERSION=$1
else
  echo "Expected only a version as argument."
  SHOW_USAGE=y
fi

if [[ "${IMG_FAMILY}" == "" ]] ; then
  IMG_FAMILY=${IMG_FAMILY_DEF}
fi
if [[ "${IMG_USER}" == "" ]] ; then
  IMG_USER=${IMG_USER_DEF}
fi

if [[ "${IMG_FAMILY}" == "" ]] ; then
  echo "No Image family set."
  SHOW_USAGE=y
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
  echo "Usage: $0 [OPTIONS] <version>"
  echo ""
  echo "Options:"
  echo "  --img-family <name>       The name for the image-family. Default is \"${IMG_FAMILY_DEF}\"."
  echo "  --img-user <username>     The username for the application owner. Default is \"${IMG_USER_DEF}\"."
  exit 1
fi

mkdir -p target
sed -e s/__IMG_USER__/${IMG_USER}/g -e s/__IMG_FAMILY__/${IMG_FAMILY}/g < axonserver-enterprise/src/main/gce/axoniq-axonserver.conf > target/axoniq-${IMG_FAMILY}.conf
sed -e s/__IMG_USER__/${IMG_USER}/g -e s/__IMG_FAMILY__/${IMG_FAMILY}/g < axonserver-enterprise/src/main/gce/axonserver.properties > target/axonserver.properties
sed -e s/__IMG_USER__/${IMG_USER}/g < axonserver-enterprise/src/main/gce/setup.sh > target/setup.sh
for f in check-link.sh mount-disk.sh set-property.sh get-property-value.sh get-property-names.sh startup.sh ; do cp axonserver-enterprise/src/main/gce/${f} target/${f} ; done
chmod 755 target/*.sh

if [ ! -s axonserver-enterprise/target/axonserver-enterprise-${IMG_VERSION}-exec.jar ] ; then
  getLastFromNexus -v ${IMG_VERSION} -o axonserver-enterprise/target/axonserver-enterprise-${IMG_VERSION}-exec.jar io.axoniq.axonserver axonserver-enterprise
fi
chmod 755 axonserver-enterprise/target/${IMG_FAMILY}-${IMG_VERSION}-exec.jar
if [ ! -s target/axonserver-cli.jar ] ; then
  getLastFromNexus -v ${IMG_VERSION} -o target/axonserver-cli.jar io.axoniq.axonserver axonserver-cli
fi
chmod 755 target/axonserver-cli.jar
