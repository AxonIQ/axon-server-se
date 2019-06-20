#!/bin/bash

# Copyright (c) 2018 by Axoniq B.V. - All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export AXONSERVER_HOME=/opt/axonserver
cd ${AXONSERVER_HOME}

echo >> ${AXONSERVER_HOME}/axonserver.properties

# can be provided through Docker/Kubernetes:
# - AXONSERVER_NAME
if [ "x${AXONSERVER_NAME}" != "x" ] ; then
  echo "axoniq.axonserver.name=${AXONSERVER_NAME}" >> ${AXONSERVER_HOME}/axonserver.properties
fi
# - AXONSERVER_HOSTNAME
if [ "x${AXONSERVER_HOSTNAME}" != "x" ] ; then
  echo "axoniq.axonserver.hostname=${AXONSERVER_HOSTNAME}" >> ${AXONSERVER_HOME}/axonserver.properties
fi
# - AXONSERVER_DOMAIN
if [ "x${AXONSERVER_DOMAIN}" != "x" ] ; then
  echo "axoniq.axonserver.domain=${AXONSERVER_DOMAIN}" >> ${AXONSERVER_HOME}/axonserver.properties
fi
# - AXONSERVER_HTTP_PORT
if [ "x${AXONSERVER_HTTP_PORT}" != "x" ] ; then
  echo "server.port=${AXONSERVER_HTTP_PORT}" >> ${AXONSERVER_HOME}/axonserver.properties
fi
# - AXONSERVER_GRPC_PORT
if [ "x${AXONSERVER_GRPC_PORT}" != "x" ] ; then
  echo "axoniq.axonserver.port=${AXONSERVER_GRPC_PORT}" >> ${AXONSERVER_HOME}/axonserver.properties
fi
# - AXONSERVER_TOKEN
if [ "x${AXONSERVER_TOKEN}" != "x" ] ; then
  echo "axoniq.axonserver.accesscontrol.enabled=true" >> ${AXONSERVER_HOME}/axonserver.properties
  echo "axoniq.axonserver.accesscontrol.token=${AXONSERVER_TOKEN}" >> ${AXONSERVER_HOME}/axonserver.properties
else
  echo "axoniq.axonserver.accesscontrol.enabled=false" >> ${AXONSERVER_HOME}/axonserver.properties
  echo "axoniq.axonserver.accesscontrol.token=" >> ${AXONSERVER_HOME}/axonserver.properties
fi
# - AXONSERVER_EVENTSTORE
if [ "x${AXONSERVER_EVENTSTORE}" != "x" ] ; then
  echo "axoniq.axonserver.event.storage=${AXONSERVER_EVENTSTORE}" >> ${AXONSERVER_HOME}/axonserver.properties
fi
# - AXONSERVER_LOGSTORE
if [ "x${AXONSERVER_LOGSTORE}" != "x" ] ; then
  echo "axoniq.axonserver.replication.log-storage-folder=${AXONSERVER_LOGSTORE}" >> ${AXONSERVER_HOME}/axonserver.properties
fi
# - AXONSERVER_CONTROLDB
if [ "x${AXONSERVER_CONTROLDB}" != "x" ] ; then
  echo "axoniq.axonserver.controldb-path=${AXONSERVER_CONTROLDB}" >> ${AXONSERVER_HOME}/axonserver.properties
fi

# - JAVA_OPTS
if [ "x${JAVA_OPTS}" = "x" ] ; then
  export JAVA_OPTS=-Xmx512m
fi

java -Djava.security.egd=file:/dev/./urandom ${JAVA_OPTS} -jar ${AXONSERVER_HOME}/axonserver.jar