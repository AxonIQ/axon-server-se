# JDBC Storage Engine

_Experimental_

Stores events in a relational database. Currently tested with H2, MySQL and Oracle.

## Set up

In axonserver.properties set the following properties to use a relational database as store:

    axoniq.axonserver.storage=jdbc
    axoniq.axonserver.storage.jdbc.url=<ConnectionUrl>
    axoniq.axonserver.storage.jdbc.driver=<DriverClass>
    axoniq.axonserver.storage.jdbc.user=<UserId to connect to the database>
    axoniq.axonserver.storage.jdbc.password=<Password to connect to the database>
    axoniq.axonserver.storage.jdbc.multi-context-strategy=schema-per-context or single-schema
    axoniq.axonserver.storage.jdbc.store-on-leader-only=true or false
    

### Multi-context-strategy

When storing the events in a relational database there are 2 options on how to separate the data per 
context:

1. schema-per-context, creates a separate schema per context (for Oracle this is a separate user per context)
   
1. single-schema, creates an event and a snapshot table per context in the current schema

Note that the JDBC storage engine uses the same connection information for all contexts, so the user
connecting to the database needs to have the right credentials to create the objects or they have to 
be pre-created.    


## Usage from IntelliJ

Add dependency to axonserver-enterprise module:

        <dependency>
            <groupId>io.axoniq.axonserver</groupId>
            <artifactId>axonserver-jdbc-storage</artifactId>
            <version>${project.parent.version}</version>
            <scope>runtime</scope>
        </dependency>

In module axonserver-jdbc-storage ensure that the correct database drivers are added.

In settings -> Build, Execution, Deployment -> Compiler -> Annotation Processors disable annotation 
processing for module axonserver-jdbc-storage.

## Usage from command line

Add axonserver-jdbc-storage.jar and the database driver jdbc jars to the exts directory 
and start axonserver.jar.

