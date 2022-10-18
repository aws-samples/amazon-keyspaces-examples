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
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.col

object GlueApp {

  def main(sysArgs: Array[String]) {

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "KEYSPACE_NAME", "TABLE_NAME", "DRIVER_CONF", "DISTINCT_KEYS").toArray)

    val driverConfFileName = args("DRIVER_CONF")

    val conf = new SparkConf()
        .setAll(
         Seq(
            ("directJoinSetting", "off"),
            ("spark.cassandra.connection.config.profile.path",  driverConfFileName),
            ("spark.cassandra.query.retry.count", "100"),

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

    val connector = CassandraConnector.apply(conf);
    val session = connector.openSession();
    val peersCount = session.execute("SELECT * FROM system.peers").all().size()

    val partitioner = session.execute("SELECT partitioner from system.local").one().getString("partitioner")


    logger.info("Total number of seeds:" + peersCount);
    logger.info("Configured partitioner:" + partitioner);

    if(peersCount == 0){
       throw new Exception("No system peers found. Check required permissions to read from the system.peers table. If using VPCE check permissions for describing VPCE endpoints. ")
    }

    if(partitioner.equals("com.amazonaws.cassandra.DefaultPartitioner")){
        throw new Exception("Sark requires the use of RandomPartitioner or Murmur3Partitioner. See Working with partioners in Amazon Keyspaces documentation. https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-partitioners.html")
    }


    val tableName = args("TABLE_NAME")
    val keyspaceName = args("KEYSPACE_NAME")

    val tableDf = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> tableName, "keyspace" -> keyspaceName, "pushdown" -> "false"))
      .load()

    val distinctKeys = args("DISTINCT_KEYS").filterNot(_.isWhitespace).split(",")

    logger.info("distinctKeys: " + distinctKeys.mkString(", "))

    val results = tableDf.select(distinctKeys.head, distinctKeys.tail:_*).distinct().count()

    logger.info("Total number of distinct keys: " + results)


    Job.commit()
  }
}
