#!/bin/bash

if [[ $# != 2 ]] ; then
    echo "Usage: $0 <username> <src-dir>"
    exit 1
fi

USERNAME=$1
SRC_DIR=$2

adduser -d /var/lib/${USERNAME} -U ${USERNAME}
cp -r ${SRC_DIR}/* /var/lib/${USERNAME}/
mkdir -p /var/log/${USERNAME}
chown -R ${USERNAME}:${USERNAME} /var/lib/${USERNAME} /var/log/${USERNAME}

if [ -s /var/lib/${USERNAME}/${USERNAME}.service ] ; then
    cp /var/lib/${USERNAME}/${USERNAME}.service /etc/systemd/system/${USERNAME}.service
    systemctl enable ${USERNAME}.service
fi
