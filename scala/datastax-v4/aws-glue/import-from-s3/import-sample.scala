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



object GlueApp {

  def main(sysArgs: Array[String]) {

  val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "KEYSPACE_NAME", "TABLE_NAME", "DRIVER_CONF", "FORMAT", "S3_URI").toArray)

  val driverConfFileName = args("DRIVER_CONF")

  val conf = new SparkConf()
      .setAll(
       Seq(
           ("spark.task.maxFailures",  "10"),

          ("spark.cassandra.output.consistency.level",  "LOCAL_QUORUM"),
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

    import com.datastax.spark.connector._
    import org.apache.spark.sql.cassandra._
    import sparkSession.implicits._

    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val tableName = args("TABLE_NAME")
    val keyspaceName = args("KEYSPACE_NAME")
    val backupFormat = args("FORMAT")

   val s3bucketBackupsLocation = args("S3_URI")

   val orderedData = sparkSession.read.format(backupFormat).load(s3bucketBackupsLocation)

   //You want randomize data before loading to maximize table throughput and avoid WriteThottleEvents
   //Data exported from another database or Cassandra may be ordered by primary key.
   //With Amazon Keyspaces you want to load data in a random way to use all available resources.
   //The following command will randomize the data.
   val shuffledData = orderedData.orderBy(rand())

   shuffledData.write.format("org.apache.spark.sql.cassandra").mode("append").option("keyspace", keyspaceName).option("table", tableName).save()

    Job.commit()
  }
}
