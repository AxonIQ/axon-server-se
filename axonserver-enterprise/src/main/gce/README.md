# Setup of the Axon Server Enterprise VM image

## Axon Server Installation Root and Ownership

* Axon Server is installed under "`/var/lib/axonserver`", owned by a user "axonserver" belonging to group "`axonserver`".
* All Read-Only files can be accessed locally in "`/var/lib/axonserver`", but may be symbolic links.
* For configuration, Axon Server will use "`axonserver.properties`" (rather than the YAML variant).
* Axon Server is installed as a `systemd` service, with three scripts:

    * As a pre-start script, the "data" and "events" disks are fromatted and mounted if needed. Mount info is added to "`/etc/fstab`" to allow then to be remounted automatically after reboots.
    * A start script, which is run as user "`axonserver`".
    * A stop script, also run as user "`axonserver`", which checks for the "`AxonIQ.pid`" file to determine the actual process id.

* The Axon Server startup script will, at first run, create symbolic links to the data and events disks.
* An "`axonserver.properties`" file is provided with the following settings:

    * Logging is configured to use "`/var/log/axonserver/axonserver.log`".
    * Clustering is enabled.
    * Event and Snapshot storage is configured to use "`./events`", which is a symbolic link pointing to the (root of the) events disk.
    * The Control DB path is set to "`./control`", which is a symbolic link to a subdirectory named "`control`" on the data disk.
    * The replication log path is set to "`./log`", which is a symbolic link to a subdirectory named "`log`" on the data disk.

* On every start of the Axon Server Service:

    * A license file is retrieved from the instance metadata (key "`axoniq-license`") and stored as "`axoniq.license`".
    * Property overrides/additions are also retrieved from the instance metadata (key "`axonserver-properties`"), and applied to "`axonserver.properties`".
    * Axon Server itself is started with standard output and error redirected to "`/dev/null`", so logging does not end up in "`/var/log/messages`".

## Logging

* VMs will have "`google-fluentd`" installed, with a filter named "`axonserver`" aimed at "`/var/log/axonserver/axonserver.log`".

## Storage

* Disks have names "<server-name>-events" (for the event store files) and "<server-name>-data" (for the control DB and the replication logs).
* All disks are mounted under "`/mnt/`<disk-name>". Formatting and mounting the disks is the responsibility of the "setup" script, which is stored in the VMs meta-data and runs at VM startup. The "`startup.sh`" script can safely assume the disks are mounted and ready for use.
* Axon Server will use directories local to its installation. It is the responsibility of the "`startup.sh`" script to create symlinks to the mounted disks.

## Networking

The image makes *no* assumptions concerning networking.

## Example instance creation

```
gcloud compute --project=axoniq-platform instances create cloud-axonserver-1 \
    --zone=europe-west4-a --machine-type=n1-standard-2 --subnet=axoniq-platform-vpc --private-network-ip=10.164.0.50 --no-address \
    --metadata-from-file=axoniq-license=axoniq.license,axonserver-properties=cloud-axonserver-1.properties \
    --image=axonserver-4-3-rc1 --image-project=axoniq-devops --boot-disk-size=10GB --boot-disk-type=pd-standard \
    --disk=name=axonserver-test-events,device-name=axonserver-test-events,mode=rw,boot=no \
    --disk=name=axonserver-test-data,device-name=axonserver-test-data,mode=rw,boot=no
```