

object GlueApp {

  def main(sysArgs: Array[String]) {

    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession

    // @params: [JOB_NAME, KEYSPACE_NAME, TABLE_NAME, S3_URI_FULL_CHANGE, S3_URI_CURRENT_CHANGE, S3_URI_CURRENT_CHANGE, S3_URI_NEW_CHANGE]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "KEYSPACE_NAME", "TABLE_NAME", "S3_URI_FULL_CHANGE", "S3_URI_CURRENT_CHANGE", "S3_URI_NEW_CHANGE").toArray)
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