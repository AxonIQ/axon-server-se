#!/bin/bash
if [ `hostname -s` = "axon-server-0" -o `hostname -s` = "axon-server" ]; then
	echo "First node in cluster - no registration"
else 
	echo "Waiting for node to come up" 
	HEALTH_CHECK_RETURN=1
	while [ $HEALTH_CHECK_RETURN -ne 0 ]; do
		wget --quiet --spider http://localhost:8024/health
		HEALTH_CHECK_RETURN=$?
		sleep 5s
	done
	echo "Registering this node with node 0" 
	java -jar cli.jar register-node -S http://localhost:8024 -h axon-server-0.`hostname -d` -p 8224
fi
