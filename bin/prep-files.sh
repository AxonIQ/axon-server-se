#!/bin/bash

SHOW_USAGE=n

VERSION=
CLI_VERSION=
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
  elif [[ "$1" == "--cli-version" ]] ; then
    if [[ $# -gt 1 ]] ; then
      CLI_VERSION=$2
      shift 2
    else
      echo "Missing version after \"--cli-version\"."
      SHOW_USAGE=y
    fi
  else
    echo "Unknown option \"$1\"."
    SHOW_USAGE=y
  fi

done

if [[ $# == 1 ]] ; then
    SERVER_VERSION=$1
else
    echo "Expected only a version as argument."
    SHOW_USAGE=y
fi

if [[ "${CLI_VERSION}" == "" ]] ; then
    echo "WARNING: Assuming CLI has version \"${SERVER_VERSION}\"."
    CLI_VERSION=${SERVER_VERSION}
fi
if [[ "${TARGET}" == "" ]] ; then
    TARGET=${TARGET_DEF}
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
    echo "Usage: $0 [OPTIONS] <version>"
    echo ""
    echo "Options:"
    echo "  --target <dir-name>       The name for the target directory. Default is \"${TARGET_DEF}\"."
    echo "  --cli-version <version>   The version of the Axon Server CLI. Default is to use the Axon Server EE version."
    exit 1
fi

if [ -d ${TARGET} ] ; then
    rm -rf ${TARGET}
fi
mkdir -p ${TARGET}
mkdir -p ${TARGET}/exts

wget -O ${TARGET}/exts/jar_files.zip "https://download.jar-download.com/cache_jars/com.google.cloud/google-cloud-logging-logback/0.116.0-alpha/jar_files.zip"

if [ ! -s ${TARGET}/exts/jar_files.zip ] ; then
    echo "ERROR: Could not download a google-cloud-logging-logback jars from https://download.jar-download.com."
    exit 1
fi

unzip -d ${TARGET}/exts ${TARGET}/exts/jar_files.zip

for f in setup.sh setup-user.sh startup.sh shutdown.sh axonserver.service check-link.sh mount-disk.sh \
         axonserver.properties set-property.sh get-property-value.sh get-property-names.sh \
         logback-spring.xml
do
    cp axonserver-enterprise/src/main/gce/${f} ${TARGET}/${f}
done

if [ -s axonserver-enterprise/target/axonserver-enterprise-${SERVER_VERSION}-exec.jar ] ; then
    cp axonserver-enterprise/target/axonserver-enterprise-${SERVER_VERSION}-exec.jar ${TARGET}/axonserver.jar
else
    getLastFromNexus -v ${SERVER_VERSION} -o ${TARGET}/axonserver.jar io.axoniq.axonserver axonserver-enterprise
fi
if [ ! -s ${TARGET}/axonserver.jar ] ; then
    echo "ERROR: Could not find Axon Server EE version \"${SERVER_VERSION}\"."
    exit 1
fi

getLastFromNexus -v ${CLI_VERSION} -o ${TARGET}/axonserver-cli.jar io.axoniq.axonserver axonserver-cli
if [ ! -s ${TARGET}/axonserver-cli.jar ] ; then
    echo "ERROR: Could not find Axon Server CLI version \"${CLI_VERSION}\"."
    exit 1
fi

chmod 755 ${TARGET}/*.{sh,jar}
