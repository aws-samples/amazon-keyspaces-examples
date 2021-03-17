# Amazon Keyspaces VPC endpoint examples

 This is the example code for best practices when connecting to Amazon Keyspaces via a VPC endpoint. The example
 also utilizes the AWS SigV4 Auth plugin. For more information on the plugin see https://docs.aws.amazon.com/mcs/latest/devguide/identity-and-access-mgmt.html#IAM.Samples.Roles.

 ## Building

 In order to build the example code, you will need Maven version 3.6.3 or higher, which you can obtain from
 https://maven.apache.org/. Once you have Maven installed, you can build a shaded (all dependencies included) JAR with
 the command:

 ``` shell
 mvn package
 ```

 Once this is complete, you should have an `target/amazon-keyspaces-vpce-examples-1.0.0.jar` file.

 ## EC2

 The OrdersFetcher is a standalone class for retrieving orders from the acme.orders table, as part of the example in the
 documentation at https://docs.aws.amazon.com/mcs/latest/devguide/identity-and-access-mgmt.html#IAM.Samples.Roles.

 You can copy the shaded JAR, along with the `cassandra_truststore.jks` file, to an EC2 instance that has an associated IAM role for MCS
 access. You can execute the JAR with the command:

 ``` shell
 java -Djavax.net.ssl.trustStore=./cassandra_truststore.jks \
   -Djavax.net.ssl.trustStorePassword=amazon \
   -jar amazon-keyspaces-vpce-examples-1.0.0.jar \
   <region> <endpoint> <customer ID>
 ```

 For example, to lookup orders for our pretend customer "1234" in us-east-2:

 ``` shell
 java -Djavax.net.ssl.trustStore=./cassandra_truststore.jks \
   -Djavax.net.ssl.trustStorePassword=amazon \
   -jar amazon-keyspaces-vpce-examples-1.0.0.jar \
   us-east-2 cassandra.us-east-2.amazonaws.com 1234
 ```

 ## Further Reading

 The documentation for the AWS SigV4 Auth Plugin for Cassandra can be found at
 https://docs.aws.amazon.com/mcs/latest/devguide/programmatic.credentials.html#programmatic.credentials.SigV4_MCS.
