#!/bin/bash

HOSTNAME=$(hostname)
/opt/__IMG_USER__/mount-disk.sh ${HOSTNAME}-data __IMG_USER__
/opt/__IMG_USER__/mount-disk.sh ${HOSTNAME}-events __IMG_USER__

sudo -Hu axonserver /bin/bash -cl /opt/__IMG_USER__/startup.sh
