import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import org.apache.spark.SparkContext
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
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.{URI}

object GlueApp {
  def main(sysArgs: Array[String]) {


    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "KEYSPACE_NAME", "TABLE_NAME", "S3_URI_FULL_CHANGE", "S3_URI_CURRENT_CHANGE", "S3_URI_NEW_CHANGE", "DRIVER_CONF").toArray)
    
    val driverConfFileName = args("DRIVER_CONF")

    val conf = new SparkConf()
      .setAll(
       Seq(
          ("spark.task.maxFailures",  "10"),
             
          ("spark.cassandra.connection.config.profile.path",  driverConfFileName),
          ("spark.cassandra.query.retry.count", "1000"),
          ("spark.cassandra.output.consistency.level",  "LOCAL_QUORUM"),//WRITES
          ("spark.cassandra.input.consistency.level",  "LOCAL_QUORUM"),//READS

          //("spark.cassandra.sql.inClauseToJoinConversionThreshold", "0"),
          //("spark.cassandra.sql.inClauseToFullScanConversionThreshold", "0"),
          ("spark.cassandra.concurrent.reads", "512"),

          ("spark.cassandra.output.concurrent.writes", "15"),
          ("spark.cassandra.output.batch.grouping.key", "none"),
          ("spark.cassandra.output.batch.size.rows", "1")
      ))

    val spark: SparkContext = new SparkContext(conf)
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession
    import sparkSession.implicits._

    // @params: [JOB_NAME, KEYSPACE_NAME, TABLE_NAME, S3_URI_FULL_CHANGE, S3_URI_CURRENT_CHANGE, S3_URI_CURRENT_CHANGE, S3_URI_NEW_CHANGE]
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val tableName = args("TABLE_NAME")
    val keyspaceName = args("KEYSPACE_NAME")
    val fullDataset = args("S3_URI_FULL_CHANGE")
    val incrementalCurrentDataset = args("S3_URI_CURRENT_CHANGE")
    val incrementalNewDataset = args("S3_URI_NEW_CHANGE")

    def checkS3(path: String): Boolean = {
      FileSystem.get(URI.create(path), spark.hadoopConfiguration).exists(new Path(path))
    }

    val dfSourceAsIs = spark.cassandraTable(keyspaceName, tableName)
      .select("userid", "level", "gameid", "description", "nickname", "zip", "email", "updatetime", WriteTime("email") as "writeTime")

    // Cast to Spark datatypes, for example, all UDTs to String
    val dfSourceWithCastDataTypes = dfSourceAsIs.keyBy(row => (row.getString("userid"), row.getString("level"), row.getInt("gameid"), row.getString("description"), row.getString("nickname"), row.getString("zip"), row.getString("email"), row.getString("updatetime"), row.getStringOption("writeTime")))
      .map(x => x._1)
      .toDF("userid", "level", "gameid", "description", "nickname", "zip", "email", "updatetime", "writeTime")

    // Persist full dataset in parquet format to S3
    dfSourceWithCastDataTypes.drop("writeTime")
      .write
      .mode(SaveMode.Overwrite)
      .parquet(fullDataset)

    // Persist primarykeys and column writetimes to S3
    if (checkS3(incrementalCurrentDataset) && checkS3(incrementalNewDataset)) {
      val shortDataframeT0 = sparkSession.read.parquet(incrementalCurrentDataset)
      val shortDataframeT1 = sparkSession.read.parquet(incrementalNewDataset)
      shortDataframeT1.select("userid", "level", "gameid", "writeTime")
        .write.mode(SaveMode.Overwrite).parquet(incrementalCurrentDataset)
      dfSourceWithCastDataTypes.select("userid", "level", "gameid", "writeTime")
        .write.mode(SaveMode.Overwrite).parquet(incrementalNewDataset)
    }

    if (checkS3(incrementalCurrentDataset) && !checkS3(incrementalNewDataset)) {
      dfSourceWithCastDataTypes.select("userid", "level", "gameid", "writeTime")
        .write.mode(SaveMode.Overwrite).parquet(incrementalNewDataset)
    }

    Job.commit()
  }
}
