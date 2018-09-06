#!/bin/sh
GROUP_ID=$1
ARTIFACT=$2

HOST=dev-nexus.axoniq.io
API=service/rest/beta/search/assets

USER="-u `cat /nexus_settings/username`:`cat /nexus_settings/password`"
QUERY='?repository=products-snapshots&maven.groupId='${GROUP_ID}'&maven.artifactId='${ARTIFACT}'&maven.extension=tgz'

URL=`curl -ks ${USER} https://${HOST}/${API}${QUERY} | jq -r '.items[].downloadUrl' | sort -r | head -1`

if [ "x${URL}" != "x" ] ; then
    curl -ks ${USER} ${URL} | tar zxf -
else
    echo "Failed to retrieve URL of asset"
    exit 1
fi