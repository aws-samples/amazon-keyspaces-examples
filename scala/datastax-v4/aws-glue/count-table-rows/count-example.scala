import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
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
    
object GlueApp {

  def main(sysArgs: Array[String]) {

    val requiredParams = Seq("JOB_NAME", "KEYSPACE_NAME", "TABLE_NAME", "DRIVER_CONF")
    
    val optionalParams = Seq("DISTINCT_KEYS")

    // Build a list of optional parameters that exist in sysArgs
    val validOptionalParams = optionalParams.filter(param => sysArgs.contains(s"--$param"))

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

            ("spark.cassandra.output.concurrent.writes", "5"),
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
    
    //count rows
    val tableName = args("TABLE_NAME")
    val keyspaceName = args("KEYSPACE_NAME")
    val distinctKeysCSV = args.getOrElse("DISTINCT_KEYS", "")

    val tableDf = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> tableName, 
                    "keyspace" -> keyspaceName, 
                    "pushdown" -> "false"))//set to true when executing against Apache Cassandra, false when working with Keyspaces
      .load()
       //.filter("my_column=='somevalue' AND my_othercolumn=='someothervalue'")

    if (Option(distinctKeysCSV).exists(_.nonEmpty)) {
      
      val distinctKeys = distinctKeysCSV.filterNot(_.isWhitespace).split(",")

      val total = tableDf.select(distinctKeys.head, distinctKeys.tail:_*).distinct().count()

      logger.info("Total number of distinct rows:" + total)

    } else {
      
      val total =  tableDf.toJavaRDD.count()

      logger.info("Total number of rows:" + total)
    }
    

    

    Job.commit()
  }
}
