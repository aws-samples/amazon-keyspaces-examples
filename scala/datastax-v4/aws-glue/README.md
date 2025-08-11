## Working with AWS Glue and Amazon Keyspaces 

Using AWS Glue and the Spark Cassandra Connector, develoeprs can create repatable large scala data operations against Amazon Keyspaces tables using serverless resources. In the following repository we setup Glue to leverage the Spark Cassandra connector, and have different examples to perform common ETL functions such as import, export, count, and transform. 


### Getting Started

We created a simple shell script to setup the Spark Cassandra connector with Glue. The following script takes three optional parameters. 
* ```SETUP_STACKNAME``` which will be used to create resources with cloudformation. The SETUP_STACKNAME used here will be import when deploying cloudformation scripts in patterns modules contained within this repository. 
* ```S3_BUCKET_NAME``` which defines the s3 bucket used to store the Spark Cassandra Connector artifacts. 
* ```GLUE_SERVICE_ROLE_NAME``` defines the name for the service role used to run Glue jobs that connect to Amazon Keyspaces and S3. 

```shell
./setup-connector.sh SETUP_STACKNAME S3_BUCKET_NAME GLUE_SERVICE_ROLE_NAME

```

The script perfroms the following steps:
* Creates an S3 bucket for storing required jars and confirguation
* Creates an IAM role to use Glue to access Keyspaces and S3 using CloudFormation
* Downloads the [Spark Cassandra connector](https://github.com/datastax/spark-cassandra-connector) and uploads it to s3 
* Downloads the [Sigv4 Authentication plugin](https://github.com/aws/aws-sigv4-auth-cassandra-java-driver-plugin) and uploads it to s3
* Downloads the [spark extensions](https://github.com/G-Research/spark-extension) and uploads it to s3
* Uses git to download [Keyspaces Retry Policy](https://github.com/aws-samples/amazon-keyspaces-java-driver-helpers) and compile it using maven, finally uploads artifact to s3
* Uploads a driver configuration for connection to Amazon Keyspaces [keyspaces-application.conf](keyspaces-application.conf)
* Uploads a driver configuration for connection to Apache Cassandra [cassandra-application.conf](cassandra-application.conf)


The resulting directory structure takes on the following shape. The required jars related to the spark cassandra connector, sigv4 plugin, Keyspaces retry-policy, and spark extensions reside in the jars directory. The conf contains driver configurations for connecting to Amazon Keyspaces or self managed cassandra. Scripts, spark-logs, and export will be used for individual glue jobs. 

```
  S3  
    \--- S3_BUCKET
            \------- jars
                        \--- spark cassandra connector
                        \--- spark extensions
                        \--- keyspaces sigv4 driver plugin
                        \--- keyspaces retry policy 
            \------- conf
                        \--- keyspaces-properties.conf
                        \--- cassandra-properties.conf
            \------- scripts
            \------- spark-logs
            \------- export

 IAM
    \--- GLUE SERVICE ROLE
            \-------- glue service permissions
            \-------- s3 read\write access 
            \-------- Keyspaces read\write access

```
### Region configuration
You will need to update the ```keyspaces-application.conf``` configuration file for the AWS region where you will be executing glue jobs. 
  * __basic.contact-points__ - Amazon Keyspaces service endpoint
  * __basic.load-balancing-policy.local-datacenter__ - Load balancing policy
  * __advanced.auth-provider.aws-region__ - Sigv4 auth provider 

https://github.com/aws-samples/amazon-keyspaces-examples/blob/621beef936899509d6dfd071526971974f803d19/scala/datastax-v4/aws-glue/keyspaces-application.conf#L4-L22
 
### Update the partitioner for your account
In Apache Cassandra, partitioners control which nodes data is stored on in the cluster. Partitioners create a numeric token using a hashed value of the partition key. Cassandra uses this token to distribute data across nodes.  To use Apache Spark or AWS glue you may need to update the partitioner if set to DefaultPartitioner or RandomPartitioner to Mumur3Partitioner. You can execute this CQL command from the Amazon Keyspaces console [CQL editor](https://console.aws.amazon.com/keyspaces/home#cql-editor)
 
```shell
SELECT partitioner FROM system.local;

UPDATE system.local set partitioner='org.apache.cassandra.dht.Murmur3Partitioner' where key='local';
```
For more info see [Working with partitioners](https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-partitioners.html)


### Modules
 * [export-to-s3](export-to-s3) - Export Cassandra table to S3
