#!/bin/bash
if [ `hostname -s` = "axonserver-enterprise-0" -o `hostname -s` = "axonserver-enterprise" ]; then
	echo "Waiting for node to come up" 
	HEALTH_CHECK_RETURN=1
	while [ $HEALTH_CHECK_RETURN -ne 0 ]; do
		wget --quiet --spider http://localhost:8024/v1/public
		HEALTH_CHECK_RETURN=$?
		sleep 5s
	done
	echo "Adding default user and app"
	
	wget -O - http://localhost:8024/v1/applications/admin
	if [ $? -ne 0 ]; then
		java -jar cli.jar register-application -a admin -r ADMIN,READ,WRITE -T token-for-my-test-cluster
		java -jar cli.jar register-user -u admin -p hello -r ADMIN,READ
	fi
fi
