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
          ("spark.task.maxFailures",  "100"),
             
          ("spark.cassandra.connection.config.profile.path",  driverConfFileName),
          ("spark.cassandra.query.retry.count", "1000"),
          ("spark.cassandra.output.consistency.level",  "LOCAL_QUORUM"),//WRITES
          ("spark.cassandra.input.consistency.level",   "LOCAL_QUORUM"),//READS

          ("spark.cassandra.sql.inClauseToJoinConversionThreshold", "0"),
          ("spark.cassandra.sql.inClauseToFullScanConversionThreshold", "0"),
          ("spark.cassandra.concurrent.reads", "512"),
          ("spark.cassandra.input.split.sizeInMB",  "64")
          
          ("spark.cassandra.output.concurrent.writes", "15"),
          ("spark.cassandra.output.batch.grouping.key", "none"),
          ("spark.cassandra.output.batch.size.rows", "1"),

      ))

    val spark: SparkContext = new SparkContext(conf)

    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession
    import sparkSession.implicits._

    // @params: [JOB_NAME, KEYSPACE_NAME, TABLE_NAME, S3_URI_FULL_CHANGE, S3_URI_CURRENT_CHANGE, S3_URI_CURRENT_CHANGE, S3_URI_NEW_CHANGE]
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    def checkS3(path: String): Boolean = {
      return FileSystem.get(URI.create(path), spark.hadoopConfiguration).exists(new Path(path))
    }

    val tableName = args("TABLE_NAME")
    val keyspaceName = args("KEYSPACE_NAME")
    val fullDataset = args("S3_URI_FULL_CHANGE")
    val incrementalCurrentDataset = args("S3_URI_CURRENT_CHANGE")
    val incrementalNewDataset = args("S3_URI_NEW_CHANGE")

    val fullDf = sparkSession.read.parquet(fullDataset)

    if (checkS3(incrementalCurrentDataset) && !checkS3(incrementalNewDataset)) {
      fullDf.write.format("org.apache.spark.sql.cassandra").mode("append").option("keyspace", keyspaceName).option("table", tableName).save()

    }

    if (checkS3(incrementalCurrentDataset) && checkS3(incrementalNewDataset)) {
      val shortDataframeT1 = sparkSession.read.parquet(incrementalNewDataset)
      val shortDataframeT0 = sparkSession.read.parquet(incrementalCurrentDataset)

      val inserts = shortDataframeT1.as("T1").join(shortDataframeT0.as("T0"), $"T1.userid" === $"T0.userid" && $"T1.level" === $"T0.level" && $"T1.gameid" === $"T0.gameid", "leftanti")
      val finalInserts = inserts.as("INSERTED").join(fullDf.as("ORIGINAL"), $"INSERTED.userid" === $"ORIGINAL.userid" && $"INSERTED.level" === $"ORIGINAL.level" && $"INSERTED.gameid" === $"ORIGINAL.gameid", "inner").selectExpr("ORIGINAL.*").drop("writeTime")
      finalInserts.write.format("org.apache.spark.sql.cassandra").mode("append").option("keyspace", keyspaceName).option("table", tableName).save()

      val updates = shortDataframeT0.as("T0").join(shortDataframeT1.as("T1"), $"T1.userid" === $"T0.userid" && $"T1.level" === $"T0.level" && $"T1.gameid" === $"T0.gameid", "inner").filter($"T1.writeTime" > $"T0.writetime").select($"T1.wheventid", $"T1.name", $"T1.endpointid", $"T1.writeTime")
      val finalUpdates = updates.as("UPDATED").join(fullDf.as("ORIGINAL"), $"UPDATED.userid" === $"ORIGINAL.userid" && $"UPDATED.level" === $"ORIGINAL.level" && $"UPDATED.gameid" === $"ORIGINAL.gameid", "inner").selectExpr("ORIGINAL.*").drop("writeTime")
      finalUpdates.write.format("org.apache.spark.sql.cassandra").mode("append").option("keyspace", keyspaceName).option("table", tableName).save()

      val finalDeletes = shortDataframeT0.as("T0").join(shortDataframeT1.as("T1"), $"T1.userid" === $"T0.userid" && $"T1.level" === $"T0.level" && $"T1.gameid" === $"T0.gameid", "leftanti").drop("writeTime")

      finalDeletes.rdd.foreach(d => {
        val userid = d.get(0).toString
        val level = d.get(1).toString
        val gameid = d.get(2).toString
        val whereClause = f"$userid%s and '$level%s' and $gameid%s"
        spark.cassandraTable(keyspaceName, tableName).where(whereClause).deleteFromCassandra(keyspaceName, tableName)
      })

    }
    Job.commit()
  }
}
