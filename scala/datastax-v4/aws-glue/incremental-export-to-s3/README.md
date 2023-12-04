## Using Glue Incremental Export and Import Example
This example provides scala script for creating incremental exports. This can be used to migrate data from Apache Cassandra to Amazon  Keyspaces or create custom backups. Given two exports from different time periods, this script will create a diff consisting of subset consisting of INSERTS, UPDATES, DELETES. By storing the incremental differences one can reduced the overall size of incremental backups.  This example is dependent on previous exports created by [export example](../export-to-s3). This repo leverages diff functionality found in [spark extensions project](https://github.com/G-Research/spark-extension). 

![incremental export](incremental-export.png)

### Setup incremental export to S3 
The following script sets up AWS Glue job to create a diff between two different exports. To export data see our [export example](../export-to-s3)

* PARENT_STACK_NAME is the stack name used to create the spark cassandra connector with Glue. [setup script](../)
* EXPORT_STACK_NAME is the stack name used to create export glue job. 
* KEYSPACE_NAME and TABLE_NAME Keyspaces and table is the fully qualified name of the table you wish to export.
* S3URI is the S3 uri where the data will be exported.  
* FORMAT can be json, csv, or parquet. parquet is recommended for ease of use with data loading, transformations, and using exports with Athena.
* DISTINCT_KEYS the primary key for the dataset in comma seperated list pk1,pk2,ck1,ck2
* PASTURI is the S3 uri of the older export to be compared to
* CURRENTURI is the S3 uri of the newer export to be compared to

```sh
./setup-incremental-export.sh PARENT_STACK_NAME EXPORT_STACK_NAME KEYSPACE_NAME TABLE_NAME S3URI FORMAT DISTINCT_KEYS PASTURI CURRENTURI

```

### Setup incremental import from S3 
The following script sets up AWS Glue job to import an incremental export in s3 to Keyspaces table. This script is different than the import script as it will import changes specified by the [spark extensions project](https://github.com/G-Research/spark-extension).  The script takes the following parameters.

* PARENT_STACK_NAME is the stack name used to create the spark cassandra connector with Glue. [setup script](../)
* EXPORT_STACK_NAME is the stack name used to create export glue job. 
* KEYSPACE_NAME and TABLE_NAME Keyspaces and table is the fully qualified name of the table you wish to export.
* S3URI is the S3 uri where the data will be exported.  
* FORMAT can be json, csv, or parquet. parquet is recommended for ease of use with data loading, transformations, and using exports with Athena.
* DISTINCT_KEYS the primary key for the dataset in comma seperated list pk1,pk2,ck1,ck2

```sh
./setup-incremental-import.sh PARENT_STACK_NAME EXPORT_STACK_NAME KEYSPACE_NAME TABLE_NAME S3URI FORMAT DISTINCT_KEYS

```

### Export file format
The format created includes the type of change (I,D,C,N) and the changed value. In the export example it will remove value N (not changed) before storing to s3. On import I (insert, c update) will be inserted/updated while D will result in deletes. original cells are prefixed with left_ and new cells are prefixed with right_. In this example we remove the orignal values and only use the new values. Original values can be useful for validation or other business logic.  

* I is INSERT
* D is DELETE 
* C is UPDATE Both previous and new values are prefixed with left_ and right_.
* N is Not changed. We can filter it out to reduce data size. 

```json
    {"diff":"I","id":"-67214788834858333A","create_date":"2023-08-01T18:23:59.476Z","right_count":1,"right_data": "-67214788834858333A"}
    {"diff":"D","id":"-7134365464711695755","create_date":"2023-02-25T19:08:26.000Z","left_count":-823388298404028328, "left_data":"-7444764033894103459"}
    {"diff":"C","id":"-672147888348583332","create_date":"2023-02-27T20:04:30.000Z","left_count":-2365493309516205255, "right_count":1,"left_data":"-1643515377232023460","right_data":"-1643515377232023460"}
    {"diff":"I","id":"4053134097057361633","create_date":"2023-08-01T18:28:34.454Z","right_count":1,"right_data":"4053134097057361633"}

```

## Putting it all togeter
The following example shows all four scripts needed to perform incremental export and import. 
1. setup connector with setup-connector.sh
2. setup export with setup-export.sh
3. setup import with setup-import.sh
4. setup incremental wit setup-incremental-export.sh
5. setup incremental wit setup-incremental-import.sh
 
You can chain export, inremental export, and incremental import through Glue workflow. Work flow can be started upon the occurrence of a single Amazon EventBridge event (the S3 PutObject operation). With this trigger type, AWS Glue export and import jobs can be an event consumer based on new data placed in an s3 bucket. 

```sh

./setup-connector.sh incremental

./setup-export.sh incremental export tlp_stress random_access s3://amazon-keyspaces-glue/export/ parquet

./setup-import.sh incremental export tlp_stress random_access s3://amazon-keyspaces-glue/export/snapshot/year=2023/month=11/day=22/hour=14/minute=09/  parquet

./setup-incremental-export.sh incremental export tlp_stress random_access s3://amazon-keyspaces-glue/export/ parquet partition_id,row_id s3://amazon-keyspaces-glue/export/snapshot/year=2023/month=11/day=22/hour=14/minute=09/ s3://amazon-keyspaces-glue/export/snapshot/year=2023/month=11/day=22/hour=19/minute=57/

./setup-incremental-import.sh incremental import tlp_stress random_access s3://amazon-keyspaces-glue/export/incremental/year=2023/month=11/day=22/hour=20/minute=09/ parquet partition_id,row_id

```