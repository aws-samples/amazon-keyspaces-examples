import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql._
import com.datastax.oss.driver.api.core._
import com.amazonaws.services.glue.log.GlueLogger
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets


object GlueApp {

  def compress(data: Array[Byte], algorithm: String): Array[Byte] = {
    algorithm.toUpperCase match {
      case "ZSTD" =>
        com.github.luben.zstd.Zstd.compress(data)
      case "LZ4" =>
        val factory = net.jpountz.lz4.LZ4Factory.fastestInstance()
        val compressor = factory.fastCompressor()
        val maxLen = compressor.maxCompressedLength(data.length)
        val compressed = new Array[Byte](maxLen + 4)
        val originalLen = java.nio.ByteBuffer.allocate(4).putInt(data.length).array()
        System.arraycopy(originalLen, 0, compressed, 0, 4)
        val compressedLen = compressor.compress(data, 0, data.length, compressed, 4, maxLen)
        compressed.take(4 + compressedLen)
      case "SNAPPY" =>
        org.xerial.snappy.Snappy.compress(data)
      case "GZIP" =>
        val bos = new java.io.ByteArrayOutputStream()
        val gzip = new java.util.zip.GZIPOutputStream(bos)
        gzip.write(data)
        gzip.close()
        bos.toByteArray
      case _ =>
        throw new IllegalArgumentException(s"Unsupported compression algorithm: $algorithm. Supported: ZSTD, LZ4, SNAPPY, GZIP")
    }
  }

  def main(sysArgs: Array[String]) {

    val requiredParams = Seq("JOB_NAME", "KEYSPACE_NAME", "SOURCE_TABLE", "TARGET_TABLE", "DRIVER_CONF")
    val optionalParams = Seq("COMPRESSION", "GROUP_BY_COLUMN", "WHERE_CLAUSE")
    val validOptionalParams = optionalParams.filter(param => sysArgs.contains(s"--$param"))
    val allParams = requiredParams ++ validOptionalParams

    val args = GlueArgParser.getResolvedOptions(sysArgs, allParams.toArray)

    val driverConfFileName = args("DRIVER_CONF")

    val conf = new SparkConf()
        .setAll(
         Seq(
            ("spark.task.maxFailures", "100"),
            ("spark.cassandra.connection.config.profile.path", driverConfFileName),
            ("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions"),
            ("directJoinSetting", "on"),
            ("spark.cassandra.output.consistency.level", "LOCAL_QUORUM"),
            ("spark.cassandra.input.consistency.level", "LOCAL_ONE"),
            ("spark.cassandra.sql.inClauseToJoinConversionThreshold", "0"),
            ("spark.cassandra.sql.inClauseToFullScanConversionThreshold", "0"),
            ("spark.cassandra.concurrent.reads", "50"),
            ("spark.cassandra.output.concurrent.writes", "3"),
            ("spark.cassandra.output.batch.grouping.key", "none"),
            ("spark.cassandra.output.batch.size.rows", "1"),
            ("spark.cassandra.output.ignoreNulls", "true")
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

    logger.info("Total number of seeds:" + peersCount)
    logger.info("Configured partitioner:" + partitioner)

    if(peersCount == 0){
       throw new Exception("No system peers found. Check required permissions to read from the system.peers table. If using VPCE check permissions for describing VPCE endpoints. https://docs.aws.amazon.com/keyspaces/latest/devguide/vpc-endpoints.html")
    }

    if(partitioner.equals("com.amazonaws.cassandra.DefaultPartitioner")){
        throw new Exception("Spark requires the use of RandomPartitioner or Murmur3Partitioner. See Working with partitioners in Amazon Keyspaces documentation. https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-partitioners.html")
    }

    val keyspaceName = args("KEYSPACE_NAME")
    val sourceTable = args("SOURCE_TABLE")
    val targetTable = args("TARGET_TABLE")
    val compressionAlgo = args.getOrElse("COMPRESSION", "ZSTD")
    val groupByColumn = args.getOrElse("GROUP_BY_COLUMN", "")
    val whereClause = args.getOrElse("WHERE_CLAUSE", "")

    logger.info(s"Source: $keyspaceName.$sourceTable")
    logger.info(s"Target: $keyspaceName.$targetTable")
    logger.info(s"Compression: $compressionAlgo")
    logger.info(s"Group by column: ${if (groupByColumn.isEmpty) "PARTITION (no clustering group)" else groupByColumn}")
    logger.info(s"Where clause: ${if (whereClause.isEmpty) "NONE (full table scan)" else whereClause}")

    var tableDf = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> sourceTable,
                   "keyspace" -> keyspaceName,
                   "pushdown" -> "false"))
      .load()

    if(whereClause.trim.nonEmpty){
       tableDf = tableDf.filter(whereClause)
    }

    val tableMetadata = session.getMetadata.getKeyspace(keyspaceName).get()
      .getTable(sourceTable).get()

    val partitionKeys = tableMetadata.getPartitionKey.asScala.map(_.getName.toString).toList
    val clusteringColumns = tableMetadata.getClusteringColumns.asScala.keys.map(_.getName.toString).toList

    logger.info(s"Partition keys: ${partitionKeys.mkString(", ")}")
    logger.info(s"Clustering columns: ${clusteringColumns.mkString(", ")}")

    val groupColumns: List[String] = if (groupByColumn.isEmpty) {
      partitionKeys
    } else {
      if (!clusteringColumns.contains(groupByColumn)) {
        throw new IllegalArgumentException(
          s"GROUP_BY_COLUMN '$groupByColumn' is not a clustering column. " +
          s"Available clustering columns: ${clusteringColumns.mkString(", ")}")
      }
      val clusteringIndex = clusteringColumns.indexOf(groupByColumn)
      partitionKeys ++ clusteringColumns.take(clusteringIndex + 1)
    }

    logger.info(s"Grouping by columns: ${groupColumns.mkString(", ")}")

    val allColumns = tableDf.columns.toList

    val withJsonCol = tableDf.withColumn("_row_json", to_json(struct(allColumns.map(col): _*)))

    val compressUdf = udf((jsonRows: Seq[String]) => {
      val jsonArray = "[" + jsonRows.mkString(",") + "]"
      compress(jsonArray.getBytes(StandardCharsets.UTF_8), compressionAlgo)
    })

    val compressedDf = withJsonCol
      .groupBy(groupColumns.map(col): _*)
      .agg(compressUdf(collect_list(col("_row_json"))).as("data"))

    compressedDf
      .write
      .format("org.apache.spark.sql.cassandra")
      .mode("append")
      .option("keyspace", keyspaceName)
      .option("table", targetTable)
      .save()

    session.close()
    Job.commit()
  }
}
