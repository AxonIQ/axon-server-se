#!/bin/bash

SHOW_USAGE=n

VERSION=
TARGET=
TARGET_DEF=target/packer

while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

  if [[ "$1" == "--target" ]] ; then
    if [[ $# -gt 1 ]] ; then
      TARGET=$2
      shift 2
    else
      echo "Missing directory name after \"--target\"."
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

if [[ "${TARGET}" == "" ]] ; then
  TARGET=${TARGET_DEF}
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
  echo "Usage: $0 [OPTIONS] <version>"
  echo ""
  echo "Options:"
  echo "  --target <dir-name>       The name for the target directory. Default is \"${TARGET_DEF}\"."
  exit 1
fi

if [ -d ${TARGET} ] ; then
  rm -rf ${TARGET}
fi
mkdir -p ${TARGET}

for f in setup.sh startup.sh shutdown.sh axonserver.service axoniq-axonserver.conf axonserver.properties check-link.sh mount-disk.sh set-property.sh get-property-value.sh get-property-names.sh ; do
  cp axonserver-enterprise/src/main/gce/${f} ${TARGET}/${f}
done

if [ -s axonserver-enterprise/target/axonserver-enterprise-${IMG_VERSION}-exec.jar ] ; then
  cp axonserver-enterprise/target/axonserver-enterprise-${IMG_VERSION}-exec.jar ${TARGET}/axonserver.jar
else
  getLastFromNexus -v ${IMG_VERSION} -o ${TARGET}/axonserver.jar io.axoniq.axonserver axonserver-enterprise
fi
getLastFromNexus -v ${IMG_VERSION} -o ${TARGET}/axonserver-cli.jar io.axoniq.axonserver axonserver-cli

chmod 755 ${TARGET}/*.{sh,jar}
