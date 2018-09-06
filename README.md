# Messaging platform
Distributed messaging platform for events, commands and queries

## Distributed setup

All nodes can serve requests. Requests may be forwarded from one node to the next.

Bounded contexts?

## Command routing

Each application subscribes to the messaging platform and provides information about the commands it can handle.
Applications dispatch commands over the AxonIQCommandBus queryDefinition bus, commands are sent to the messaging platform and 
dispatched to one application capable of handling the queryDefinition.

Commands contain a routing key, which is used by the queryDefinition dispatcher in the messaging platform to determine the handler. 
When there are no changes in the active applications, queryDefinition with the same routing key are sent to the same handler.
If there are new handlers added, the only a minimal number of routing keys per handler should be rerouted.

### Command failure

Command execution failure in application vs. infra failure. 
Must be clear to calling application what kind of error occurred. 

### Command store
Stores commands (and responses?). Techniques used similar to EventStore.
Seperate process? 
Async 

## Event routing

Messaging platform is a proxy for the event store. Applications can connect the event store to the messaging platorm.
The interface to the event dispatcher in the messaging platform is the same as to the EventStore.

ToDo: 
- blacklist action on the open stream
- bundling of tracking event processors
 
### Blacklisting

When an application has no handlers for a specific event type it can send a blacklist message to the EventDispatcher on the messaging platform.
From this moment on, it will no longer send these events to the application. This blacklist is only kept for the time the application is active, when the
application reconnects to the messaging platform the blacklist is empty.

### Event failure

## Query routing


## Development Notes

Enable IntelliJ IDEA support for ECMA6:
File -> Settings -> Languages and Frameworks -> JavaScript -> JavaScript language version = ECMAScript6

For fast development of UI with WebPack and Vue-Loader, use "npm run watch".


