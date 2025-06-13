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
import com.datastax.spark.connector.cql._
import com.datastax.oss.driver.api.core._
import org.apache.spark.sql.functions.rand
import com.amazonaws.services.glue.log.GlueLogger
import java.time.ZonedDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.time.format.DateTimeFormatter


object GlueApp {

  def main(sysArgs: Array[String]) {

    val requiredParams = Seq("JOB_NAME", "KEYSPACE_NAME", "TABLE_NAME", "DRIVER_CONF")

    val optionalParams = Seq("DISTINCT_KEYS", "QUERY_FILTER", "FORMAT", "S3_URI")

    // Build a list of optional parameters that exist in sysArgs
    val validOptionalParams = optionalParams.filter(param =>  sysArgs.contains(s"--$param") && param.trim.nonEmpty)
  
    // Combine required and valid optional parameters
    val validParams = requiredParams ++ validOptionalParams

    val args = GlueArgParser.getResolvedOptions(sysArgs, validParams.toArray)
    
    val driverConfFileName = args("DRIVER_CONF")

    val conf = new SparkConf()
        .setAll(
         Seq(
             ("spark.task.maxFailures",  "100"),
          
            ("spark.cassandra.connection.config.profile.path",  driverConfFileName),
            ("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions"),
            ("directJoinSetting", "on"),
            
            ("spark.cassandra.output.consistency.level",  "LOCAL_QUORUM"),//WRITES
            ("spark.cassandra.input.consistency.level",  "LOCAL_ONE"),//READS

            ("spark.cassandra.sql.inClauseToJoinConversionThreshold", "0"),
            ("spark.cassandra.sql.inClauseToFullScanConversionThreshold", "0"),
            ("spark.cassandra.concurrent.reads", "50"),

            ("spark.cassandra.output.concurrent.writes", "3"),
            ("spark.cassandra.output.batch.grouping.key", "none"),
            ("spark.cassandra.output.batch.size.rows", "1"),
            ("spark.cassandra.output.batch.size.rows", "1"),
            ("spark.cassandra.output.ignoreNulls", "true")
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
    
    logger.info("Total number of seeds:" + peersCount)
    logger.info("Configured partitioner:" + partitioner)
    
    if(peersCount == 0){
       throw new Exception("No system peers found. Check required permissions to read from the system.peers table. If using VPCE check permissions for describing VPCE endpoints. https://docs.aws.amazon.com/keyspaces/latest/devguide/vpc-endpoints.html")
    }
    
    if(partitioner.equals("com.amazonaws.cassandra.DefaultPartitioner")){
        throw new Exception("Spark requires the use of RandomPartitioner or Murmur3Partitioner. See Working with partitioners in Amazon Keyspaces documentation. https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-partitioners.html")
    }
    
    val backupLocation = args.getOrElse("S3_URI", "")
    val backupFormat = args.getOrElse("FORMAT", "parquet")
    val filterCriteria = args.getOrElse("QUERY_FILTER", "")
    
    val tableName = args("TABLE_NAME")
    val keyspaceName = args("KEYSPACE_NAME")
    
    
    val query =
        s"""
           |SELECT column_name, kind
           |FROM system_schema.columns
           |WHERE keyspace_name = '$keyspaceName' AND table_name = '$tableName';
           |""".stripMargin

     // Execute the query
     val resultSet = session.execute(query)
      
     val validKinds = Set("partition_key", "clustering")
     // Extract primary key column names
      
      val primaryKeyColumnsCSV = resultSet.all().asScala
        .filter(row => validKinds.contains(row.getString("kind")))
        .map(_.getString("column_name"))
        .toList
        .mkString(", ")
    
     // Output the primary key columns
    logger.info(s"Primary Key Columns for $keyspaceName.$tableName: ${primaryKeyColumnsCSV}")
    
    val distinctKeys = args.getOrElse("DISTINCT_KEYS", primaryKeyColumnsCSV).filterNot(_.isWhitespace).split(",")
    
    // Output the primary key columns
    logger.info(s"Primary Key Columns for $keyspaceName.$tableName: ${distinctKeys.mkString(", ")}")

    var tableDf = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> tableName, 
                    "keyspace" -> keyspaceName, 
                    "pushdown" -> "false"))//set to true when executing against Apache Cassandra, false when working with Keyspaces
      .load()
      
    if(filterCriteria.trim.nonEmpty){
       tableDf = tableDf.filter(filterCriteria)
    }

    //backup to s3 for data that wil be deleted
    if(backupLocation.trim.nonEmpty){
       val now = ZonedDateTime.now( ZoneOffset.UTC )//.truncatedTo( ChronoUnit.MINUTES ).format( DateTimeFormatter.ISO_DATE_TIME )

       //backup location for deletes
       val fullbackuplocation = backupLocation +
                               "/export" + 
                               "/" + keyspaceName +
                               "/" + tableName +
                               "/bulk-delete" +
                               "/year="   +  "%04d".format(now.getYear()) +
                               "/month="  +  "%02d".format(now.getMonthValue()) + 
                               "/day="    +  "%02d".format(now.getDayOfMonth()) +
                               "/hour="   +  "%02d".format(now.getHour()) + 
                               "/minute=" +  "%02d".format(now.getMinute())


      
      tableDf.write.format(backupFormat).mode(SaveMode.ErrorIfExists).save(fullbackuplocation)
    }

    tableDf.select(distinctKeys.head, distinctKeys.tail:_*).rdd.deleteFromCassandra(keyspaceName, tableName)

    Job.commit()
  }
}
