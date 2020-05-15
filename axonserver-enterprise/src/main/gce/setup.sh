#!/bin/bash

HOSTNAME=$(hostname)
/var/lib/axonserver/mount-disk.sh ${HOSTNAME}-data axonserver
/var/lib/axonserver/mount-disk.sh ${HOSTNAME}-events axonserver

