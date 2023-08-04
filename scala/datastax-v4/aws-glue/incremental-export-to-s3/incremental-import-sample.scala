import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger
import scala.collection.JavaConverters._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.SaveMode._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql._
import com.datastax.oss.driver.api.core._
    
import org.apache.spark.sql.functions.rand

import com.amazonaws.services.glue.log.GlueLogger



object GlueApp {

  def main(sysArgs: Array[String]) {

  val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "KEYSPACE_NAME", "TABLE_NAME", "DRIVER_CONF", "FORMAT", "S3_URI","DISTINCT_KEYS").toArray)

  val driverConfFileName = args("DRIVER_CONF")

  val conf = new SparkConf()
      .setAll(
       Seq(
           ("spark.task.maxFailures",  "10"),
          
          ("spark.cassandra.connection.config.profile.path",  driverConfFileName),
          ("spark.cassandra.query.retry.count", "1000"),

          ("spark.cassandra.sql.inClauseToJoinConversionThreshold", "0"),
          ("spark.cassandra.sql.inClauseToFullScanConversionThreshold", "0"),
          ("spark.cassandra.concurrent.reads", "512"),

          ("spark.cassandra.output.concurrent.writes", "15"),
          ("spark.cassandra.output.batch.grouping.key", "none"),
          ("spark.cassandra.output.batch.size.rows", "1")
      ))

    val spark: SparkContext = new SparkContext(conf)
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession

    import sparkSession.implicits._

    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    
    val logger = new GlueLogger
    
    //validation steps for peers and partitioner 
    val connector = CassandraConnector.apply(conf);
    val session = connector.openSession();
    val peersCount = session.execute("SELECT * FROM system.peers").all().size()
    
    val partitioner = session.execute("SELECT partitioner from system.local").one().getString("partitioner")
    
    logger.info("Total number of seeds:" + peersCount);
    logger.info("Configured partitioner:" + partitioner);
    
    if(peersCount == 0){
       throw new Exception("No system peers found. Check required permissions to read from the system.peers table. If using VPCE check permissions for describing VPCE endpoints. https://docs.aws.amazon.com/keyspaces/latest/devguide/vpc-endpoints.html")
    }
    
    if(partitioner.equals("com.amazonaws.cassandra.DefaultPartitioner")){
        throw new Exception("Sark requires the use of RandomPartitioner or Murmur3Partitioner. See Working with partioners in Amazon Keyspaces documentation. https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-partitioners.html")
    }
    
    val tableName = args("TABLE_NAME")
    val keyspaceName = args("KEYSPACE_NAME")
    val backupFormat = args("FORMAT")
    val s3bucketBackupsLocation = args("S3_URI")
    val distinctKeys = args("DISTINCT_KEYS").filterNot(_.isWhitespace).split(",")


    //read the 
    val orderedData = sparkSession.read.format(backupFormat).load(s3bucketBackupsLocation)  

   //You want randomize data before loading to maximize table throughput and avoid WriteThottleEvents 
   //Data exported from another database or Cassandra may be ordered by primary key.  
   //With Amazon Keyspaces you want to load data in a random way to use all available resources.  
   //The following command will randomize the data. 
   val shuffledData = orderedData.orderBy(rand())
   
   // the spark extension used in the incremental export will keep both original and new cells in the diff.
   //original cells are prefixed with left_ and new cells are prefixed with right_. 
   //in this example we remove the orignal values and only use the new values. Original values can be useful 
   // for validation or other business logic. 
   val columns_to_include = shuffledData.columns.filter(!_.startsWith("left_"))
   
   val onlyNewValues = shuffledData.select(columns_to_include.head, columns_to_include.tail: _*)
   
   //To match the schema of the table we remove the prefix for the new values.
   val new_column_names= onlyNewValues.columns.map(_.replace("right_", ""))
   
   val cleanSchemaOfDiffMeta =  onlyNewValues.toDF(new_column_names: _*)
  
   //Next we seperate inserts and updates and deletes. I for insert, C for update, D for delete.
   //After filtering we also drop the diff column to match the schema of the table. 
   val insertsAndUpdates = cleanSchemaOfDiffMeta.filter($"diff" === "I" || $"diff" === "C" ).drop("diff")
   
   //Save inserts and updates to Keyspaces
   insertsAndUpdates.write.format("org.apache.spark.sql.cassandra").mode("append").option("keyspace", keyspaceName).option("table", tableName).save()
   
    //Similar, we need to extract deletes and execute them using the rdd functions. You need to select keycolumns. 
    //There are also options to delete individual fields. THis can be needed if column values are deleted instead of rows. 
    val deletes = cleanSchemaOfDiffMeta.filter($"diff" === "D").drop("diff")
    
    //distinctkeys passed in as a parameter
    deletes.select(distinctKeys.head, distinctKeys.tail:_*).rdd.deleteFromCassandra(keyspaceName, tableName)

   Job.commit()
  }
}
