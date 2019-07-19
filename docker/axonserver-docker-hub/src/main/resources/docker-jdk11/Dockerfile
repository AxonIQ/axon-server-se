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
FROM openjdk:11-jdk

ENV AXONSERVER_HOME /opt/axonserver
RUN mkdir -p ${AXONSERVER_HOME}/data

COPY axonserver.jar ${AXONSERVER_HOME}/axonserver.jar
COPY axonserver.properties ${AXONSERVER_HOME}/axonserver.properties
COPY axonserver-cli.jar ${AXONSERVER_HOME}/axonserver-cli.jar
COPY startup.sh ${AXONSERVER_HOME}/startup.sh
COPY ["AXONIQ OPEN SOURCE LICENSE.txt", "${AXONSERVER_HOME}/AXONIQ OPEN SOURCE LICENSE.txt"]
COPY ["APACHE LICENSE v2.0.txt", "${AXONSERVER_HOME}/APACHE LICENSE v2.0.txt"]
RUN chmod 755 ${AXONSERVER_HOME}/startup.sh

EXPOSE 8024
EXPOSE 8124
VOLUME [ "${AXONSERVER_HOME}/data" ]
VOLUME [ "${AXONSERVER_HOME}/log" ]

WORKDIR ${AXONSERVER_HOME}
CMD [ "/bin/sh", "-c", "${AXONSERVER_HOME}/startup.sh" ]