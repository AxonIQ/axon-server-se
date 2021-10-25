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
TAG_LATEST=n
TAG_LATEST_JDK=n
TAG_JDK=n
TAG_VERSION=y
PLATFORMS=
PLATFORMS_OVR=
PLATFORMS_DEF=linux/amd64,linux/arm64

Usage() {
    echo "Usage: $0 [OPTIONS] <version> [<tag>]"
    echo "  --dev-only         Only generate 'dev' images. (including shell) Shorthand: '-d'."
    echo "  --no-dev           Only generate non-'dev' images. (without shell, if possible) Shorthand: '-n'."
    echo "  --full             Generate all image variants. This is the default mode."
    echo "  --tag-latest       Add 'latest' tags."
    echo "  --tag-latest-jdk   Add 'latest' tags that specify the JDK version. Implies '--tag-jdk'."
    echo "  --tag-jdk          Add tags that specify the JDK version."
    echo "  --no-tag-version   Do NOT add a tag with only the version."
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

options=$(getopt -l 'dev-only,no-dev,full,tag-latest,no-tag-latest,tag-jdk,no-tag-jdk,tag-latest-jdk,tag-version,no-tag-version,push,settings:,local,jdk:,distroless,temurin,openjdk,ubi8,repo:,platforms:' -- 's:dn' "$@")
[ $? -eq 0 ] || {
  Usage
}
eval set -- "$options"
while true; do
    case $1 in
    --dev-only|-d)     MODE=dev ;;
    --no-dev|-n)       MODE=prod ;;
    --full)            MODE=full ;;
    --tag-latest)      TAG_LATEST=y ;;
    --no-tag-latest)   TAG_LATEST=n ;;
    --tag-latest-jdk)  TAG_LATEST_JDK=y; TAG_JDK=y; ;;
    --tag-jdk)         TAG_JDK=y ;;
    --no-tag-jdk)      TAG_JDK=n ;;
    --tag-version)     TAG_VERSION=y ;;
    --no-tag-version)  TAG_VERSION=n ;;
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

if [[ "${TAG_LATEST}" == "y" && "${TAG_JDK}" == "y" ]] ; then
    TAG_LATEST_JDK=y
fi

if [[ "${PLATFORMS}" == "" ]] ; then
    PLATFORMS=${PLATFORMS_DEF}
fi
if [[ "${PLATFORMS_OVR}" != "" ]] ; then
    PLATFORMS=${PLATFORMS_OVR}
fi

TAG="${IMG_BASE}:${VERSION}"
JDK_TAG="${IMG_BASE}:${VERSION}-jdk-${JDK}"
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
cp axonserver/LICENSE "${TGT}/AXONIQ OPEN SOURCE LICENSE.txt"

if [[ "${MODE}" == "full" || "${MODE}" == "prod" ]] ; then

    echo "Building '${TAG}' from '${BASE}'."
    sed -e "s+__BASE_IMG__+${BASE}+g" ${SRC}/build/Dockerfile > ${TGT}/Dockerfile

    TAGS=
    if [[ "${TAG_VERSION}" == "y" ]] ; then
        TAGS="${TAGS} -t ${TAG}"
    fi
    if [[ "${TAG_JDK}" == "y" ]] ; then
        TAGS="${TAGS} -t ${JDK_TAG}"
    fi
    if [[ "${TAG_LATEST}" == "y" ]] ; then
        TAGS="${TAGS} -t ${LATEST_TAG}"
    fi
    if [[ "${TAG_LATEST_JDK}" == "y" ]] ; then
        TAGS="${TAGS} -t ${LATEST_TAG}-jdk-${JDK}"
    fi

    if [[ "${PUSH}" == "y" ]] ; then
        docker buildx build --platform ${PLATFORMS} --push ${TAGS} ${TGT}
    else
        docker build ${TAGS} ${TGT}
    fi

    echo "Building '${TAG}-nonroot' from '${BASE_NONROOT}'."
    sed -e "s+__BASE_IMG__+${BASE_NONROOT}+g" ${SRC}/build-nonroot/Dockerfile > ${TGT}/Dockerfile

    TAGS=
    if [[ "${TAG_VERSION}" == "y" ]] ; then
        TAGS="${TAGS} -t ${TAG}-nonroot"
    fi
    if [[ "${TAG_JDK}" == "y" ]] ; then
        TAGS="${TAGS} -t ${JDK_TAG}-nonroot"
    fi
    if [[ "${TAG_LATEST}" == "y" ]] ; then
        TAGS="${TAGS} -t ${LATEST_TAG}-nonroot"
    fi
    if [[ "${TAG_LATEST_JDK}" == "y" ]] ; then
        TAGS="${TAGS} -t ${LATEST_TAG}-jdk-${JDK}-nonroot"
    fi

    if [[ "${PUSH}" == "y" ]] ; then
        docker buildx build --platform ${PLATFORMS} --push ${TAGS} ${TGT}
    else
        docker build ${TAGS} ${TGT}
    fi

fi

if [[ "${MODE}" == "full" || "${MODE}" == "dev" ]] ; then

    echo "Building '${TAG}-dev' from '${BASE_DEV}'."
    sed -e "s+__BASE_IMG__+${BASE_DEV}+g" ${SRC}/build/Dockerfile > ${TGT}/Dockerfile

    TAGS=
    if [[ "${TAG_VERSION}" == "y" ]] ; then
        TAGS="${TAGS} -t ${TAG}-dev"
    fi
    if [[ "${TAG_JDK}" == "y" ]] ; then
        TAGS="${TAGS} -t ${JDK_TAG}-dev"
    fi
    if [[ "${TAG_LATEST}" == "y" ]] ; then
        TAGS="${TAGS} -t ${LATEST_TAG}-dev"
    fi
    if [[ "${TAG_LATEST_JDK}" == "y" ]] ; then
        TAGS="${TAGS} -t ${LATEST_TAG}-jdk-${JDK}-dev"
    fi

    if [[ "${PUSH}" == "y" ]] ; then
        docker buildx build --platform ${PLATFORMS} --push ${TAGS} ${TGT}
    else
        docker build ${TAGS} ${TGT}
    fi

    echo "Building '${TAG}-dev-nonroot' from '${BASE_DEV}'."
    sed -e "s+__BASE_IMG__+${BASE_DEV_NONROOT}+g" ${SRC}/build-nonroot/Dockerfile > ${TGT}/Dockerfile

    TAGS=
    if [[ "${TAG_VERSION}" == "y" ]] ; then
        TAGS="${TAGS} -t ${TAG}-dev-nonroot"
    fi
    if [[ "${TAG_JDK}" == "y" ]] ; then
        TAGS="${TAGS} -t ${JDK_TAG}-dev-nonroot"
    fi
    if [[ "${TAG_LATEST}" == "y" ]] ; then
        TAGS="${TAGS} -t ${LATEST_TAG}-dev-nonroot"
    fi
    if [[ "${TAG_LATEST_JDK}" == "y" ]] ; then
        TAGS="${TAGS} -t ${LATEST_TAG}-jdk-${JDK}-dev-nonroot"
    fi

    if [[ "${PUSH}" == "y" ]] ; then
        docker buildx build --platform ${PLATFORMS} --push ${TAGS} ${TGT}
    else
        docker build ${TAGS} ${TGT}
    fi

fi
