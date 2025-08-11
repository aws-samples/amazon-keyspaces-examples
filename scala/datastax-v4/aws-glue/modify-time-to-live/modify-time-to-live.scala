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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


object GlueApp {

  //currentTTL is the time left on the record
  //timeToAdd time the delta add or subtract. Use negative number for subtraction. 
  def addTimeToExistingTTL(currentTTL: Int, timeToAdd: Int): Int = {
   
    val finalTTLValue = currentTTL + timeToAdd;

    // Scenario where the future ttl is less than the remaininng TTL.
    // Moving from 60 to 90 days. 
    // TODO: May be more efficient to just delete, than modify/expire 
    Math.max(1, finalTTLValue) 
  }

  //update the row with the new ttl using LWT
  //to update the ttl we must overwrite using the same row values 
  //Using LWT to check the value has not changed since reading the row for the current ttl. 
  def updateRowWithLWT(row: Row, connector: CassandraConnector): Unit = {
    //open seach creates a session or updates a reference counter on shared session. 
    val session = connector.openSession()
    
    val query =
      """UPDATE tlp_stress.keyvalue
        |USING TTL ?
        |SET value = ?
        |WHERE key = ?
        |IF value = ?""".stripMargin

    //prepared statmeents are cached by the driver, and not an issue if called multiple times. 
    val prepared = session.prepare(query)

    val key = row.getAs[String]("key")
    val value = row.getAs[String]("value")
    val expectedValue = row.getAs[String]("value")
    val ttl = row.getAs[Int]("ttlCol")

    //bind the values to the prepared statement. 
    val bound = prepared.bind(
      java.lang.Integer.valueOf(ttl), 
      value, key, expectedValue)
    
    val result = session.execute(bound)

    // Optional: check whether LWT succeeded
    if (!result.wasApplied()) {
      println(s"Conditional update failed for id=$key")
      // Here you may want want to:
      //1. read the latest row and ttl
      //2. apply the correct ttl 
      //3. use LWT to avoid conflicts
    }
    session.close()
  }

  def main(sysArgs: Array[String]) {

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "KEYSPACE_NAME", "TABLE_NAME", "DRIVER_CONF", "TTL_FIELD", "TTL_TIME_TO_ADD").toArray)

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

            ("spark.cassandra.output.concurrent.writes", "5"),
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
        throw new Exception("Sark requires the use of RandomPartitioner or Murmur3Partitioner. See Working with partioners in Amazon Keyspaces documentation. https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-partitioners.html")
    }
    
    val tableName = args("TABLE_NAME")
    val keyspaceName = args("KEYSPACE_NAME")
    val backupS3 = args("S3_URI")
    val backupFormat = args("FORMAT")
    
    val tableDf = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> tableName, 
                    "keyspace" -> keyspaceName, 
                    "pushdown" -> "false"))//set to true when executing against Apache Cassandra, false when working with Keyspaces
      .load()
      //.filter("my_column=='somevalue' AND my_othercolumn=='someothervalue'")

    // Register the UDF for calculating TTL
    val calculateTTLUDF = udf((currentTTL: Int, timeToAdd: Int) => addTimeToExistingTTL(currentTTL, timeToAdd))
    
    val timeToAdd = args("TTL_TIME_TO_ADD").toInt
    val ttlField = args("TTL_FIELD")
    // val timeToAdd = 5 * 365 * 24 * 60 * 60 //add 5 years
    //val timeToAdd = -1 * 365 * 24 * 60 * 60 //subtract 1 year
    // Calculate TTL values
    val tableDfWithTTL = tableDf
      .withColumn("ttlCol", calculateTTLUDF(ttl(col(ttlField)), lit(timeToAdd)))
    
    tableDfWithTTL.foreachPartition { partition: Iterator[Row] =>
      partition.foreach { row => updateRowWithLWT(row, connector) }
    }
    
    Job.commit()
  }
}
