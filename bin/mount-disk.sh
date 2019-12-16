#!/bin/bash

SHOW_USAGE=n

DISK_NAME=
DISK_DEV=
DISK_MOUNT_BASE=
DISK_MOUNT_BASE_DEF=/opt
DISK_MOUNT_PATH=
USER_NAME=

while [[ "${SHOW_USAGE}" == "n" && $# -gt 0 && $(expr "x$1" : x-) = 2 ]] ; do

  if [[ "$1" == "--mount" ]] ; then
    if [[ $# -gt 1 ]] ; then
      DISK_MOUNT_PATH=$2
      shift 2
    else
      echo "Missing path after \"--mount\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--mountBase" ]] ; then
    if [[ $# -gt 1 ]] ; then
      DISK_MOUNT_BASE=$2
      shift 2
    else
      echo "Missing path after \"--mountBase\"."
      SHOW_USAGE=y
    fi
  elif [[ "$1" == "--dev" ]] ; then
    if [[ $# -gt 1 ]] ; then
      DISK_DEV=$2
      shift 2
    else
      echo "Missing device path after \"--dev\"."
      SHOW_USAGE=y
    fi
  else
    echo "Unknown option \"$1\"."
    SHOW_USAGE=y
  fi

done

if [ $# != 2 ] ; then
    echo "Missing disk name and/or user name."
    SHOW_USAGE=y
else
    DISK_NAME=$1
    USER_NAME=$2
fi
if [[ "${DISK_MOUNT_NAME}" == "" ]] ; then
    DISK_MOUNT_NAME=${DISK_NAME}
fi
if [[ "${DISK_MOUNT_BASE}" ="" ]] ; then
    DISK_MOUNT_BASE=${DISK_MOUNT_BASE_DEF}
fi
if [[ "${DISK_MOUNT_PATH}" == "" ]] ; then
    DISK_MOUNT_PATH=${DISK_MOUNT_BASE}/${DISK_NAME}
fi
if [[ "${DISK_DEV}" == "" ]] ; then
    DISK_DEV=/dev/disk/by-id/google-${DISK_NAME}
    if [ ! -e ${DISK_DEV} ] ; then
        echo "No device named \"${DISK_DEV}\" found!"
        SHOW_USAGE=y
    fi
fi

if [[ "${SHOW_USAGE}" == "y" ]] ; then
    echo "Usage: $0 [OPTIONS] <disk-name> <user-name>"
    echo ""
    echo "Options:"
    echo "  --mountBase <path>  The directory where disks are mounted, default \"${DISK_MOUNT_BASE_DEF}\"."
    echo "  --mount <path>      The directory where to mount the disk, default \"<mount-base>/<disk-name>\"."
    echo "  --dev <path>        The device path of the disk, default \"/dev/disk/by-id/google-<disk-name>\"."
    exit 1
fi

DISK_UUID=`blkid -s UUID -o value ${DISK_DEV}`

if [ -d ${DISK_MOUNT_PATH} -a -d ${DISK_MOUNT_PATH}/lost+found ] ; then
    echo "Skipping disk setup after reboot."
else

    if [ ! -d ${DISK_MOUNT_PATH} ] ; then
        echo "Creating \"${DISK_MOUNT_PATH}\"."
        mkdir -p ${DISK_MOUNT_PATH}
    fi
    mnt=$(findmnt -n -o TARGET ${DISK_DEV})
    if [[ "${mnt}" == "" ]] ; then

        if [[ "${DISK_UUID}" == "" ]] ; then
            echo "Formatting disk \"${DISK_DEV}\"."
            mkfs.ext4 -m 0 -F -E lazy_itable_init=0,lazy_journal_init=0,discard ${DISK_DEV}
            DISK_UUID=`blkid -s UUID -o value ${DISK_DEV}`
            echo "Disk formatted, UUID=\"${DISK_UUID}\"."
        fi
        echo "Mounting \"${DISK_DEV}\" at \"${DISK_MOUNT_PATH}\"."
        cp /etc/fstab /etc/fstab.backup
        echo "UUID=${DISK_UUID} ${DISK_MOUNT_PATH} ext4 discard,defaults,nofail 0 2" | tee -a /etc/fstab
        mount ${DISK_MOUNT_PATH}
        echo "Disk mounted."
        chown ${USER_NAME}:${USER_NAME} ${DISK_MOUNT_PATH}
        echo "Ownership changed to ${USER_NAME} user."
    fi
    echo "Done"

fi
