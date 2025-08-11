# Amazon Keyspaces Spring examples

 This is the example code for best practices when connecting to Amazon Keyspaces with Spring Cassandra.

 This repository also utilizes the AWS SigV4 Auth plugin. For more information on the plugin see https://docs.aws.amazon.com/mcs/latest/devguide/identity-and-access-mgmt.html#IAM.Samples.Roles.

 ## Building

 In order to build the example code, you will need Maven version 3.6.3 or higher, which you can obtain from
 https://maven.apache.org/. Once you have Maven installed, you can build a shaded (all dependencies included) JAR with
 the command:

 ``` shell
 mvn package
 ```

 # The code
 ## Application Properties
 We recommend using spring data cassandra version greater than 3.0. This version uses latest cassandra driver with externalized configuration for better control and implementation of Amazon Keyspaces best practices.  In spring data application.proprties specify the property ```spring.data.cassandra.config``` with the driver application.conf see example below.

```spring.data.cassandra.config=keyspaces-application.conf```

## Driver Configuration
In the resources directory you will find ```keyspaces-application.conf``` with Amazon Keyspaces best practices for security, connections, retries, and load balancing.

## Asyncronous Resource Creation.
Amazon Keyspaces performs data definition language (DDL) operations, such as creating and deleting keyspaces and tables, asynchronously. To use spring and Amazon Keyspaces we will want to disable schema actions and create schema before launching the application. To disable schema actions, set schema action to ```None``` in your ```application.properties```.

```spring.data.cassandra.schema-action=NONE```

use the following scripts to create keyspace and table used in this example or copy the contents and paste them into the [Amazon Keyspaces CQL Editor](https://us-east-1.console.aws.amazon.com/keyspaces/home#cql-editor)
 ```
    cqlsh -f create-keyspace.cql
    cqlsh -f create-table.cql

 ```

## Sigv4 Connection
 For enhanced security, we recommend to create IAM access keys for IAM users and roles that are used across all AWS services. The Amazon Keyspaces SigV4 authentication plugin for Cassandra client drivers enables you to authenticate calls to Amazon Keyspaces using IAM access keys instead of user name and password. The sigv4 settings can be found in the ``keyspaces-application.conf```

## Using Projections
As a best practice, we recommend projecting all columns of interest in your prepared statements ```(SELECT user, fname, lname FROM user)``` rather than relying on ```SELECT *```. Both the driver and Amazon Keyspaces maintain a mapping of PreparedStatement queries to their metadata. When a table is altered there is currently no way for Amazon Keyspaces to invalidate or change the schema metadata. As a result the driver is unable to react to these changes and will not be able to correctly read rows after a schema change is made. Further, the use of the ``` @Query``` annotation can create a query method that can then be used to return projections. This type of query method can be found in ```UserRepository.java```. 

 ## Further Reading

 The documentation for the AWS SigV4 Auth Plugin for Cassandra can be found at
 https://docs.aws.amazon.com/mcs/latest/devguide/programmatic.credentials.html#programmatic.credentials.SigV4_MCS.
