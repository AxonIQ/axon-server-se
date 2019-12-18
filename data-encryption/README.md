# Data Encryption sample

This module contains a very simple and definitely not production-ready sample on
how to do encryption of data at rest in Axon Server EE. 

The result of the module is a jar file, that you can put in the exts directory of
an Axon Server installation. Axon Server will automatically pick up this 
jar file and configure a MultiContextEventTransformerFactory to create a specific
EventTransformer for each context, that does "encrypt" data before writing to file.

To enable encryption you need to specify flags in axonserver.properties file:

- axoniq.axonserver.event.flags=1
- axoniq.axonserver.snapshot.flags=1

Encryption starts working on the first segment created after setting the flags. If 
there are already segments created before the flag was set, these will remain 
unencrypted and accessible.
 