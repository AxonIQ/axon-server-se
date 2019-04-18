# AxonDB Properties Migration
This module contains the `AxonDBPropertiesMigration` class.
This class can be used to run a migration of an AxonDB properties file to an Axon Server properties file.
When running the `AxonDBPropertiesMigration`, one property can be supplied, which is the current path & name 
combination of the AxonDB property file.
If this is not provided, the file name is default to `axondb.properties`.