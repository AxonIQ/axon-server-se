#!/bin/bash

SHOW_USAGE=n

VERSION=
MVN_MODULE=
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
  elif [[ "$1" == "--mvn-module" ]] ; then
    if [[ $# -gt 1 ]] ; then
      MVN_MODULE=$2
      shift 2
    else
      echo "Missing module name after \"--mvn-module\"."
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
  echo "  --mvn-module <name>       The Maven module containing the sources and JAR. Default is the root/parent project."
  echo "  --img-family <name>       The name for the image-family. Default is \"${IMG_FAMILY_DEF}\"."
  echo "  --img-user <username>     The username for the application owner. Default is \"${IMG_USER_DEF}\"."
  exit 1
fi

mkdir -p target
sed -e s/__IMG_USER__/${IMG_USER}/g -e s/__IMG_FAMILY__/${IMG_FAMILY}/g < ${MVN_MODULE}src/main/gce/axoniq-axonserver.conf > target/axoniq-${IMG_FAMILY}.conf
sed -e s/__IMG_USER__/${IMG_USER}/g < ${MVN_MODULE}src/main/gce/setup.sh > target/setup.sh
for f in check-link.sh mount-disk.sh set-property.sh startup.sh ; do cp ${MVN_MODULE}src/main/gce/${f} target/${f} ; done
chmod 755 target/*.sh

if [ ! -s ${MVN_MODULE}target/${IMG_FAMILY}-${IMG_VERSION}-exec.jar ] ; then
  getLastFromNexus -v ${IMG_VERSION} -o ${MVN_MODULE}target/${IMG_FAMILY}-${IMG_VERSION}-exec.jar io.axoniq.axonserver axonserver-enterprise
fi
chmod 755 ${MVN_MODULE}target/${IMG_FAMILY}-${IMG_VERSION}-exec.jar
if [ ! -s target/axonserver-cli.jar ] ; then
  getLastFromNexus -v ${IMG_VERSION} -o target/axonserver-cli.jar io.axoniq.axonserver axonserver-cli
fi
chmod 755 target/axonserver-cli.jar
