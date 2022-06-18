

object GlueApp {
  def main(sysArgs: Array[String]) {

    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession

    // @params: [JOB_NAME, KEYSPACE_NAME, TABLE_NAME, S3_URI_FULL_CHANGE, S3_URI_CURRENT_CHANGE, S3_URI_CURRENT_CHANGE, S3_URI_NEW_CHANGE]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "KEYSPACE_NAME", "TABLE_NAME", "S3_URI_FULL_CHANGE", "S3_URI_CURRENT_CHANGE", "S3_URI_NEW_CHANGE").toArray)
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