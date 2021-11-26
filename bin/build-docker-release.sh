#!/bin/bash

setOpenJDK() {
    IMG_BASE=openjdk:${JDK}-slim
    IMG_BASE_DEV=openjdk:${JDK}-slim
    IMG_BASE_NONROOT=openjdk:${JDK}-slim
    IMG_BASE_DEV_NONROOT=openjdk:${JDK}-slim
    PLATFORMS=linux/amd64,linux/arm64/v8
}
setDistroless() {
    IMG_BASE=distroless:${JDK}
    IMG_BASE_DEV=distroless:${JDK}-debug
    IMG_BASE_NONROOT=openjdk:${JDK}-nonroot
    IMG_BASE_DEV_NONROOT=openjdk:${JDK}-debug-nonroot
    PLATFORMS=linux/amd64
}
setUbi8() {
    IMG_BASE=registry.access.redhat.com/ubi8/openjdk-${JDK}-runtime:latest
    IMG_BASE_DEV=registry.access.redhat.com/ubi8/openjdk-${JDK}-runtime:latest
    IMG_BASE_NONROOT=registry.access.redhat.com/ubi8/openjdk-${JDK}-runtime:latest
    IMG_BASE_DEV_NONROOT=registry.access.redhat.com/ubi8/openjdk-${JDK}-runtime:latest
    PLATFORMS=linux/amd64,linux/arm64
}
setTemurin() {
    IMG_BASE=eclipse-temurin:${JDK}-focal
    IMG_BASE_DEV=eclipse-temurin:${JDK}-focal
    IMG_BASE_NONROOT=eclipse-temurin:${JDK}-focal
    IMG_BASE_DEV_NONROOT=eclipse-temurin:${JDK}-focal
    PLATFORMS=linux/amd64,linux/arm/v7,linux/arm64/v8
}

JDK=11
BASE_TYPE=temurin
REPO_BASE=my-axonserver
TAG_BASE=
DO_PROD=n
DO_DEV=y
DO_NONROOT=y
DO_PUSH=n
SETTINGS=
LOCAL=n
TAG_LATEST=n
TAG_JDK=n
TAG_LATEST_JDK=n
TAG_VERSION=y
DO_PLATFORMS=y
PLATFORMS=
PLATFORMS_OVR=
PLATFORMS_DEF=linux/amd64,linux/arm64

Usage() {
    echo "Usage: $0 [OPTIONS] <version>"
    echo "  --prod               Generate production images, which generally do not include a shell. Shorthand: '-p'."
    echo "  --no-prod            Do not generate production images. This is the default behavior."
    echo "  --dev                Generate 'dev' images, which generally include a shell. Shorthand: '-d'. This is the default behavior."
    echo "  --no-dev             Do not generate 'dev' images."
    echo "  --nonroot            Generate 'nonroot' images, which do not run as root. Shorthand: '-n'. This is the default behavior."
    echo "  --no-nonroot         Do not generate 'nonroot' images."
    echo "  --full               Generate all image variants. Shorthand: '-f'."
    echo "  --tag-latest         Add 'latest' tags."
    echo "  --no-tag-latest      Do not add 'latest' tags."
    echo "  --tag-jdk            Add tags that specify the JDK version."
    echo "  --no-tag-jdk         Do not add tags that specify the JDK version."
    echo "  --tag-latest-jdk     Add 'latest' tags that specify the JDK version. Implies '--tag-jdk'."
    echo "  --no-tag-latest-jdk  Do not add 'latest' tags that specify the JDK version."
    echo "  --tag-version        Add a tag with only the version."
    echo "  --no-tag-version     Do not add a tag with only the version."
    echo "  --push               Push the image after building it."
    echo "  --settings <file>    Use the specified Maven 'settings.xml' to obtain credentials for Nexus. Shorthand: '-s'."
    echo "  --local              Use JAR file from the local build rather than a copy from Nexus."
    echo "  --jdk <version>      Use the specified Java version."
    echo "  --openjdk            Use the OpenJDK base images."
    echo "  --distroless         Use the Distroless base images."
    echo "  --temurin            Use the Eclipse Temurin base images. (Ubuntu Focal Fossa)"
    echo "  --ubi8               Use the RedHat Ubi8 base images."
    echo "  --repo <name>        Use this as the base for the pushed images, default 'eu.gcr.io/axoniq-devops/axonserver'"
    echo "  --platforms <list>   The platforms to build for, default is dependant on the base image."
    echo "  --no-platforms       Do not attempt to build multi-platform images."
    echo "  <version>            The Axon Server version."
    exit 1
}

options=$(getopt -l 'prod,no-prod,dev,no-dev,nonroot,no-nonroot,full,tag-latest,no-tag-latest,tag-jdk,no-tag-jdk,tag-latest-jdk,no-tag-latest-jdk,tag-version,no-tag-version,push,settings:,local,jdk:,distroless,temurin,openjdk,ubi8,repo:,platforms:,no-platforms' -- 's:pdnf' "$@")
[ $? -eq 0 ] || {
  Usage
}
eval set -- "$options"
while true; do
    case $1 in
    --prod|-p)            DO_PROD=y ;;
    --no-prod)            DO_PROD=n ;;
    --dev|-d)             DO_DEV=y ;;
    --no-dev)             DO_DEV=n ;;
    --nonroot|-n)         DO_NONROOT=y ;;
    --no-nonroot)         DO_NONROOT=n ;;
    --full|-f)            DO_PROD=y ; DO_DEV=y ; DO_NONROOT=y ;;
    --tag-latest)         TAG_LATEST=y ;;
    --no-tag-latest)      TAG_LATEST=n ;;
    --tag-jdk)            TAG_JDK=y ;;
    --no-tag-jdk)         TAG_JDK=n ;;
    --tag-latest-jdk)     TAG_LATEST_JDK=y; TAG_JDK=y; ;;
    --no-tag-latest-jdk)  TAG_LATEST_JDK=n; ;;
    --tag-version)        TAG_VERSION=y ;;
    --no-tag-version)     TAG_VERSION=n ;;
    --push)               DO_PUSH=y ;;
    --settings|-s)        SETTINGS=$2 ; shift ;;
    --local)              LOCAL=y ;;
    --jdk)                JDK=$2 ; shift ;;
    --distroless)         BASE_TYPE=distroless ;;
    --openjdk)            BASE_TYPE=openJDK ;;
    --temurin)            BASE_TYPE=temurin ;;
    --ubi8)               BASE_TYPE=ubi8 ;;
    --repo)               REPO_BASE=$2 ; shift ;;
    --platforms)          PLATFORMS_OVR=$2 ; shift ;;
    --no-platforms)       DO_PLATFORMS=n ;;
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

TAG_BASE="${REPO_BASE}:${VERSION}"
LATEST_TAG="${REPO_BASE}:latest"

EDITION=axonserver
SRC=${EDITION}/src/main/docker
TGT=target/docker
rm -rf ${TGT}
mkdir -p ${TGT}

#
# getTags()
#
# returns a list if the tags for the current image and the provided suffix.
function getTags() {
    local tagSuffix=$1

    if [[ "${TAG_VERSION}" == "y" ]] ; then
        echo ${TAG_BASE}${tagSuffix}
    fi
    if [[ "${TAG_JDK}" == "y" ]] ; then
        echo "${TAG_BASE}-jdk-${JDK}${tagSuffix}"
    fi
    if [[ "${TAG_LATEST}" == "y" ]] ; then
        echo "${LATEST_TAG}${tagSuffix}"
    fi
    if [[ "${TAG_LATEST_JDK}" == "y" ]] ; then
        echo "${LATEST_TAG}-jdk-${JDK}${tagSuffix}"
    fi
}

#
function getTagOpts() {
    local tagSuffix=$1

    for tag in $(getTags ${tagSuffix}) ; do
        echo "-t ${tag}"
    done
}

function doBuild() {
    local tagSuffix=$1
    local baseImg=$2
    local srcFile=$3

    echo "Building '${TAG_BASE}${tagSuffix}' from '${baseImg}'."

    echo "- Generating Dockerfile from '${srcFile}'"
    sed -e "s+__BASE_IMG__+${baseImg}+g" ${srcFile} > ${TGT}/Dockerfile

    local TAGS=$(getTags ${tagSuffix})
    local TAG_OPTS=$(getTagOpts ${tagSuffix})

    echo "- Running Docker build"
    if [[ "${DO_PLATFORM}" == "y" && "${DO_PUSH}" == "y" ]] ; then
        docker buildx build --platform ${PLATFORMS} --push ${TAG_OPTS} ${TGT}
    else
        docker build ${TAG_OPTS} ${TGT}
        if [[ "${DO_PUSH}" == "y" ]] ; then
            for t in ${TAGS} ; do
                docker push ${t}
            done
        fi
    fi
}

if [[ "${LOCAL}" == "y" ]] ; then

    echo "Using local build."
    cp ${EDITION}/target/${EDITION}*-exec.jar ${TGT}/axonserver.jar

else

    echo "Downloading ${EDITION} ${VERSION} from Nexus."
    if [[ "${SETTINGS}" == "" ]] ; then
        getLastFromNexus -v ${VERSION} -o ${TGT}/axonserver.jar io.axoniq.axonserver ${EDITION}
    else
        getLastFromNexus -s ${SETTINGS} -v ${VERSION} -o ${TGT}/axonserver.jar io.axoniq.axonserver ${EDITION}
    fi

fi
if [ ! -s ${TGT}/axonserver.jar ] ; then
  echo "No JAR file for Axon Server found."
  exit 1
fi

chmod 755 ${TGT}/axonserver.jar
cp ${SRC}/axonserver.properties ${TGT}/

if [[ "${DO_PROD}" == "y" ]] ; then
    doBuild "" ${IMG_BASE} ${SRC}/build/Dockerfile

    if [[ "${DO_NONROOT}" == "y" ]] ; then
      doBuild "-nonroot" ${IMG_BASE_NONROOT} ${SRC}/build-nonroot/Dockerfile
    fi
fi

if [[ "${DO_DEV}" == "y" ]] ; then

    if [ -s ${SRC}/build-dev/Dockerfile ] ; then
        doBuild "-dev" ${IMG_BASE_DEV} ${SRC}/build-dev/Dockerfile
    else
        doBuild "-dev" ${IMG_BASE_DEV} ${SRC}/build/Dockerfile
    fi

    if [[ "${DO_NONROOT}" == "y" ]] ; then
        if [ -s ${SRC}/build-dev-nonroot/Dockerfile ] ; then
            doBuild "-dev-nonroot" ${IMG_BASE_DEV_NONROOT} ${SRC}/build-dev-nonroot/Dockerfile
        else
            doBuild "-dev-nonroot" ${IMG_BASE_DEV_NONROOT} ${SRC}/build-nonroot/Dockerfile
        fi
    fi
fi
