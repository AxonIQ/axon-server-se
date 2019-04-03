# Development environment setup

## Run AxonServer cluster from IntelliJ
There are 3 run configurations to start 3 AxonServer Enterprise nodes bound with different ports: 
 * `AxonServerEnterprise Node1` uses default ports [8024, 8124, 8224]
 * `AxonServerEnterprise Node2` uses custom ports [9024, 9124, 9224]
 * `AxonServerEnterprise Node1` uses custom ports [7024, 7124, 7224]

All 3 instances start in cluster mode using the license file located in `dev/development.license`.

Instances store data inside the `dev` subdirectories:
 * `AxonServerEnterprise Node1` -> `dev/node1`
 * `AxonServerEnterprise Node2` -> `dev/node2`
 * `AxonServerEnterprise Node3` -> `dev/node3`
 
In order to configure the cluster there are following run configurations:
 * `init cluster` used to initialize the cluster with `AxonServerEnterprise Node1` and contexts `_admin` and `default`
 * `register node 2` used to register `AxonServerEnterprise Node2` with the cluster and to add it to all contexts
 * `register node 3` used to register `AxonServerEnterprise Node3` with the cluster and to add it to all contexts 
 
In order to obtain the fresh environment it is advised to delete all `dev` subdirectories.

## AxonServer UI Development Notes

Enable IntelliJ IDEA support for ECMA6:
File -> Settings -> Languages and Frameworks -> JavaScript -> JavaScript language version = ECMAScript6

For fast development of UI with WebPack and Vue-Loader, use `webpack hot rebuild` run configuration. 
This configuration starts an agent that watches for any changed `vue` files and immediately transpiles it.