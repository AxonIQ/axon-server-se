# Setup of the Axon Server Enterprise image

## Axon Server Installation Root and Ownership

* Axon Server is installed under "`/opt/axonserver`", owned by a user "axonserver" belonging to group "axonserver".
* All Read-Only files can be accessed locally in "`/opt/axonserver`", but may be symbolic links.
* For configuration, Axon Server will use "`axonserver.properties`" (rather than the YAML variant).

## Logging

* VMs will have "`google-fluentd`" installed, with 

## Storage

* Disks have names "<server-name>-events" (for the event store files) and "<server-name>-data" (for the control DB and the replication logs).
* All disks are mounted under "`/mnt/`<disk-name>". Formatting and mounting the disks is the responsibility of the "setup" script, which is stored in the VMs meta-data and runs at VM startup. The "`startup.sh`" script can safely assume the disks are mounted and ready for use.
* Axon Server will use directories local to its installation. It is the responsibility of the "`startup.sh`" script to create symlinks to the mounted disks.

```
gcloud compute --project=axoniq-devops instances create axonserver-test \
    --zone=europe-west4-a --machine-type=n1-standard-2 --subnet=axoniq-devops-vpc --private-network-ip=10.164.0.50 --no-address \
    --metadata-from-file=axoniq-license=axoniq.license 
    --metadata=startup-script=bash\ -c\ /opt/axonserver/setup.sh \
    --image=axonserver-test --image-project=axoniq-devops --boot-disk-size=10GB --boot-disk-type=pd-standard \
    --disk=name=axonserver-test-events,device-name=axonserver-test-events,mode=rw,boot=no \
    --disk=name=axonserver-test-data,device-name=axonserver-test-data,mode=rw,boot=no
```