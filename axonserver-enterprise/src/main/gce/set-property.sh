#!/bin/bash

SHOW_USAGE=n

PROPERTIES=axonserver.properties
SKIP_IF_EXISTS=n
REMOVE_OLD=y
while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

    if [[ "$1" == "--if-missing" ]] ; then
        SKIP_IF_EXISTS=y
        shift
    elif [[ "$1" == "--always-add" ]] ; then
        REMOVE_OLD=n
        shift
    elif [[ "$1" == "--properties" ]] ; then
        if [[ $# -gt 1 ]] ; then
            PROPERTIES=$2
            shift 2
        else
            echo "Expected name of properties file after \"--properties\"."
            SHOW_USAGE=y
        fi
    else
        echo "Unknown option \"$1\"."
        SHOW_USAGE=y
    fi

done

PROP=
VALUE=
if [[ $# == 2 ]] ; then
    PROP=$1
    VALUE=$2
else
    echo "Expected property name and value."
    SHOW_USAGE=y
fi

if [[ ${SHOW_USAGE} == y ]] ; then
    echo "Usage: $0 [--if-missing] [--always-add] [--properties <filename>] <property-name> <value>"
    exit 1
fi

COUNT=$(grep -ce '^[ \t]*'${PROP}'[ \t]*=.*$' ${PROPERTIES})
ADD_PROP=n
if [[ "${COUNT}" == 0 || ${SKIP_IF_EXISTS} == n ]] ; then
    rm -f ${PROPERTIES}.bak
    mv ${PROPERTIES} ${PROPERTIES}.old
    if [[ ${REMOVE_OLD} == y ]] ; then
        ( sed -e '/^[ \t]*'${PROP}'[ \t]*=.*$/d' ${PROPERTIES}.old ; echo "" ; echo "${PROP}=${VALUE}" ) | cat -s > ${PROPERTIES}
    else
        ( cat ${PROPERTIES}.old ; echo "" ; echo "${PROP}=${VALUE}" ) | cat -s > ${PROPERTIES}
    fi
else
    echo "Property exists and \"--if-missing\" used."
fi