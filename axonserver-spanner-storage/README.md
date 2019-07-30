# Google Spanner Storage Engine

_Experimental_

Stores events in a Google Spanner database 

## Set up

Authentication to Google Spanner is done using a service account file. Create one in Google IAM and 
grant Project Owner role to the account. Download the credentials file to the local machine.

Set environment variable GOOGLE_APPLICATION_CREDENTIALS to point to the location of the file.

In axonserver.properties set the following properties to use spanner as store:

    axoniq.axonserver.storage=spanner
    axoniq.axonserver.storage.spanner.project-id=<GCloud project>
    axoniq.axonserver.storage.spanner.instance=<Spanner instance>



## Usage from IntelliJ

Add dependency to axonserver-enterprise module:

        <dependency>
            <groupId>io.axoniq.axonserver</groupId>
            <artifactId>axonserver-spanner-storage</artifactId>
            <version>${project.parent.version}</version>
            <scope>runtime</scope>
        </dependency>

In settings -> Build, Execution, Deployment -> Compiler -> Annotation Processors disable annotation 
processing for module axonserver-spanner-storage.

## Usage from command line

Add axonserver-spanner-storage.jar and all required google spanner jars to the exts directory 
and start axonserver.jar.

_TODO: find out which jars are needed_