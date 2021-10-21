#!/bin/bash

setOpenJDK() {
    BASE=openjdk:${JDK}-slim
    BASE_DEV=openjdk:${JDK}-slim
    BASE_NONROOT=openjdk:${JDK}-slim
    BASE_DEV_NONROOT=openjdk:${JDK}-slim
    PLATFORMS=linux/amd64,linux/arm64/v8
}
setDistroless() {
    BASE=distroless:${JDK}
    BASE_DEV=distroless:${JDK}-debug
    BASE_NONROOT=openjdk:${JDK}-nonroot
    BASE_DEV_NONROOT=openjdk:${JDK}-debug-nonroot
    PLATFORMS=linux/amd64
}
setUbi8() {
    BASE=registry.access.redhat.com/ubi8/openjdk-${JDK}-runtime:latest
    BASE_DEV=registry.access.redhat.com/ubi8/openjdk-${JDK}-runtime:latest
    BASE_NONROOT=registry.access.redhat.com/ubi8/openjdk-${JDK}-runtime:latest
    BASE_DEV_NONROOT=registry.access.redhat.com/ubi8/openjdk-${JDK}-runtime:latest
    PLATFORMS=linux/amd64,linux/arm64
}
setTemurin() {
    BASE=eclipse-temurin:${JDK}-focal
    BASE_DEV=eclipse-temurin:${JDK}-focal
    BASE_NONROOT=eclipse-temurin:${JDK}-focal
    BASE_DEV_NONROOT=eclipse-temurin:${JDK}-focal
    PLATFORMS=linux/amd64,linux/arm/v7,linux/arm64/v8
}

JDK=11
BASE_TYPE=temorin
IMG_BASE=eu.gcr.io/axoniq-devops/axonserver
TAG=
MODE=full
PUSH=n
SETTINGS=
LOCAL=n
LATEST=n
PLATFORMS=
PLATFORMS_OVR=
PLATFORMS_DEF=linux/amd64,linux/arm64

Usage() {
    echo "Usage: $0 [OPTIONS] <version> [<tag>]"
    echo "  --dev-only         Only generate 'dev' images. (including shell) Shorthand: '-d'."
    echo "  --no-dev           Only generate non-'dev' images. (without shell, if possible) Shorthand: '-n'."
    echo "  --full             Generate all image variants. This is the default mode."
    echo "  --latest           Add image with 'latest' tags."
    echo "  --push             Push the image after building it."
    echo "  --settings <file>  Use the specified Maven 'settings.xml' to obtain credentials for Nexus. Shorthand: '-s'."
    echo "  --local            Use JAR file from the local build rather than a copy from Nexus."
    echo "  --jdk <version>    Use the specified Java version."
    echo "  --openjdk          Use the OpenJDK base images."
    echo "  --distroless       Use the Distroless base images."
    echo "  --temurin          Use the Eclipse Temurin base images. (Ubuntu Focal Fossa)"
    echo "  --ubi8             Use the RedHat Ubi8 base images."
    echo "  --platforms <list> The platforms to build for, default 'linux/amd64,linux/arm64'."
    echo "  <version>          The Axon Server version."
    exit 1
}

options=$(getopt -l 'dev-only,no-dev,full,latest,push,settings:,local,jdk:,distroless,temurin,openjdk,ubi8,repo:,platforms:' -- 's:dn' "$@")
[ $? -eq 0 ] || {
  Usage
}
eval set -- "$options"
while true; do
    case $1 in
    --dev-only|-d)     MODE=dev ;;
    --no-dev|-n)       MODE=prod ;;
    --full)            MODE=full ;;
    --latest)          LATEST=y ;;
    --push)            PUSH=y ;;
    --settings|-s)     SETTINGS=$2 ; shift ;;
    --local)           LOCAL=y ;;
    --jdk)             JDK=$2 ; shift ;;
    --distroless)      BASE_TYPE=distroless ;;
    --openjdk)         BASE_TYPE=openJDK ;;
    --temurin)         BASE_TYPE=temurin ;;
    --ubi8)            BASE_TYPE=ubi8 ;;
    --repo)            IMG_BASE=$2 ; shift ;;
    --platforms)       PLATFORMS_OVR=$2 ; shift ;;
    --)
        shift
        break
        ;;
    esac
    shift
done

if [[ $# != 1 ]] ; then
    Usage
fi
VERSION=$1

if [[ "${BASE_TYPE}" == "openJDK" ]] ; then
    setOpenJDK
elif [[ "${BASE_TYPE}" == "temurin" ]] ; then
    setTemurin
elif [[ "${BASE_TYPE}" == "ubi8" ]] ; then
    setUbi8
elif [[ "${BASE_TYPE}" == "distroless" ]] ; then
    setDistroless
else
    echo "Unknown base-type '${BASE_TYPE}'."
    Usage
fi

if [[ "${PLATFORMS}" == "" ]] ; then
    PLATFORMS=${PLATFORMS_DEF}
fi
if [[ "${PLATFORMS_OVR}" != "" ]] ; then
    PLATFORMS=${PLATFORMS_OVR}
fi

TAG="${IMG_BASE}:${VERSION}"
LATEST_TAG="${IMG_BASE}:latest"

TGT=target/docker
rm -rf ${TGT}
mkdir -p ${TGT}

SRC=axonserver/src/main/docker

if [[ "${LOCAL}" == "y" ]] ; then

    echo "Using local build."
    cp axonserver/target/axonserver*-exec.jar ${TGT}/axonserver.jar

else

    echo "Downloading Axon Server ${VERSION} from Nexus."
    if [[ "${SETTINGS}" == "" ]] ; then
        getLastFromNexus -v ${VERSION} -o ${TGT}/axonserver.jar io.axoniq.axonserver axonserver
    else
        getLastFromNexus -s ${SETTINGS} -v ${VERSION} -o ${TGT}/axonserver.jar io.axoniq.axonserver axonserver
    fi

fi
if [ ! -s ${TGT}/axonserver.jar ] ; then
  echo "No JAR file for Axon Server found."
  exit 1
fi

chmod 755 ${TGT}/axonserver.jar
cp ${SRC}/axonserver.properties ${TGT}/

if [[ "${MODE}" == "full" || "${MODE}" == "prod" ]] ; then

    echo "Building '${TAG}' from '${BASE}'."
    sed -e "s+__BASE_IMG__+${BASE}+g" ${SRC}/build/Dockerfile > ${TGT}/Dockerfile
    if [[ "${PUSH}" == "y" ]] ; then
        if [[ "${LATEST}" == "y" ]] ; then
            docker buildx build --platform ${PLATFORMS} --push -t ${TAG} -t ${LATEST_TAG} ${TGT}
        else
            docker buildx build --platform ${PLATFORMS} --push -t ${TAG} ${TGT}
        fi
    else
        docker build -t ${TAG} ${TGT}
        if [[ "${LATEST}" == "y" ]] ; then
            docker tag ${TAG} ${LATEST_TAG}
        fi
    fi

    echo "Building '${TAG}-nonroot' from '${BASE_NONROOT}'."
    sed -e "s+__BASE_IMG__+${BASE_NONROOT}+g" ${SRC}/build-nonroot/Dockerfile > ${TGT}/Dockerfile
    if [[ "${PUSH}" == "y" ]] ; then
        if [[ "${LATEST}" == "y" ]] ; then
            docker buildx build --platform ${PLATFORMS} --push -t ${TAG}-nonroot -t ${LATEST_TAG}-nonroot ${TGT}
        else
            docker buildx build --platform ${PLATFORMS} --push -t ${TAG}-nonroot ${TGT}
        fi
    else
        docker build -t ${TAG}-nonroot ${TGT}
        if [[ "${LATEST}" == "y" ]] ; then
            docker tag ${TAG}-nonroot ${LATEST_TAG}-nonroot
        fi
    fi

fi

if [[ "${MODE}" == "full" || "${MODE}" == "dev" ]] ; then

    echo "Building '${TAG}-dev' from '${BASE_DEV}'."
    sed -e "s+__BASE_IMG__+${BASE_DEV}+g" ${SRC}/build/Dockerfile > ${TGT}/Dockerfile
    if [[ "${PUSH}" == "y" ]] ; then
        if [[ "${LATEST}" == "y" ]] ; then
            docker buildx build --platform ${PLATFORMS} --push -t ${TAG}-dev -t ${LATEST_TAG}-dev ${TGT}
        else
            docker buildx build --platform ${PLATFORMS} --push -t ${TAG}-dev ${TGT}
        fi
    else
        docker build -t ${TAG}-dev ${TGT}
        if [[ "${LATEST}" == "y" ]] ; then
            docker tag ${TAG}-dev ${LATEST_TAG}-dev
        fi
    fi

    if [[ "${PUSH}" == "y" ]] ; then
        if [[ "${LATEST}" == "y" ]] ; then
            docker buildx build --platform ${PLATFORMS} --push -t ${TAG}-dev-nonroot -t ${LATEST_TAG}-dev-nonroot ${TGT}
        else
            docker buildx build --platform ${PLATFORMS} --push -t ${TAG}-dev-nonroot ${TGT}
        fi
    else
        docker build -t ${TAG}-dev-nonroot ${TGT}
        if [[ "${LATEST}" == "y" ]] ; then
            docker tag ${TAG}-dev-nonroot ${LATEST_TAG}-dev-nonroot
        fi
    fi

fi
