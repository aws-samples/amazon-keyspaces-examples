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
import scala.util.Random
import scala.collection.mutable.Map
import com.datastax.oss.driver.api.core.metadata.token.TokenRange
import com.datastax.oss.driver.api.core.metadata.token.Token
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.oss.driver.api.core.ConsistencyLevel


object GlueApp {

  def selectRandomTokenRange(session: CqlSession, keyspaceName: String, tokenSplits: Int): Map[String, Token] = {
    try {
      val logger = new GlueLogger
      logger.info(s"Selecting random token range for keyspace: $keyspaceName, token splits: $tokenSplits")
      
      // Get token ranges from the session metadata
      val tokenRanges = session.getMetadata()
        .getTokenMap()
        .get()
        .getTokenRanges()
        .asScala
        .toSet
      
      if (tokenRanges.isEmpty) {
        throw new RuntimeException("No token ranges available")
      }
      
      // Randomly select a token range from the available ranges
      val randomRange = tokenRanges.toSeq(Random.nextInt(tokenRanges.size))
      
      // Split the selected range into smaller segments for more granular sampling
      val viableSplits = randomRange.unwrap()
        .asScala
        .flatMap(_.splitEvenly(tokenSplits).asScala)
        .filter(!_.isEmpty)
        .toSet
      
      if (viableSplits.isEmpty) {
        throw new RuntimeException("No viable token splits available")
      }
      
      // Randomly select one of the split segments
      val randomSplit = viableSplits.toSeq(Random.nextInt(viableSplits.size))
      
      // Create result map with starting and ending tokens
      val result = Map[String, Token]()
      result += ("startingTokenRange" -> randomSplit.getStart)
      result += ("endingTokenRange" -> randomSplit.getEnd)
      
      logger.info(s"Selected token range: ${randomSplit.getStart} to ${randomSplit.getEnd}")
      
      result
      
    } catch {
      case e: Exception =>
        val logger = new GlueLogger
        logger.error("Failed to select random token range", e)
        throw new RuntimeException("Token range selection failed", e)
    }
  }

  def extractPartitionKeyName(session: CqlSession, keyspaceName: String, tableName: String): String = {
    try {
      val tableMetadata = session.getMetadata()
        .getKeyspace(keyspaceName)
        .get()
        .getTable(tableName)
        .get()
      
      val partitionKey = tableMetadata.getPartitionKey()
        .asScala
        .map(_.getName.toString)
        .mkString(", ")
      
      partitionKey
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to extract partition key for table $keyspaceName.$tableName", e)
    }
  }

  def sampleFromCassandra(
    session: CqlSession, 
    keyspaceName: String, 
    tableName: String, 
    tokenSplits: Int, 
    sampleSize: Int, 
    consistencyLevel: ConsistencyLevel
  ): List[com.datastax.oss.driver.api.core.cql.Row] = {
    try {
      // Select a random token range for sampling
      val tokenMap = selectRandomTokenRange(session, keyspaceName, tokenSplits)

      // Extract the partition key name for the token-based query
      val partitionKey = extractPartitionKeyName(session, keyspaceName, tableName)

      // Build query to sample records from the specified token range
      val query = s"SELECT $partitionKey FROM $keyspaceName.$tableName WHERE token($partitionKey) >= ? AND token($partitionKey) < ? LIMIT ?"
      
      // Create statement with specified consistency level for faster reads
      val simpleStatement = SimpleStatement.builder(query)
        .setConsistencyLevel(consistencyLevel)
        .addPositionalValue(tokenMap("startingTokenRange"))
        .addPositionalValue(tokenMap("endingTokenRange"))
        .addPositionalValue(sampleSize)
        .build()
      
      // Execute the query
      val resultSet = session.execute(simpleStatement)
      
      // Convert to Scala List
      val rows = resultSet.all().asScala.toList
      
      rows
    } catch {
      case e: Exception =>
        val logger = new GlueLogger
        logger.error("Failed to sample from Cassandra", e)
        throw new RuntimeException("Cassandra sampling failed", e)
    }
  }

  def main(sysArgs: Array[String]) {

    val requiredParams = Seq("JOB_NAME", "KEYSPACE_NAME", "TABLE_NAME", "DRIVER_CONF", "FORMAT", "S3_URI")
    val optionalParams = Seq("WHERE_CLAUSE")
    val validOptionalParams = optionalParams.filter(param => sysArgs.contains(s"--$param"))
    val allParams = requiredParams ++ validOptionalParams

    val args = GlueArgParser.getResolvedOptions(sysArgs, allParams.toArray)

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
    val whereClause = args.getOrElse("WHERE_CLAUSE", "")

    var tableDf = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> tableName,
                    "keyspace" -> keyspaceName,
                    "pushdown" -> "false"))//set to true when executing against Apache Cassandra, false when working with Keyspaces
      .load()

    if(whereClause.trim.nonEmpty){
       tableDf = tableDf.filter(whereClause)
    }



    val now = ZonedDateTime.now( ZoneOffset.UTC )//.truncatedTo( ChronoUnit.MINUTES ).format( DateTimeFormatter.ISO_DATE_TIME )

    val fullbackuplocation = backupS3 +
                             "/export" + 
                             "/" + keyspaceName +
                             "/" + tableName +
                             "/snapshot" +
                             "/year="   +  "%04d".format(now.getYear()) +
                             "/month="  +  "%02d".format(now.getMonthValue()) + 
                             "/day="    +  "%02d".format(now.getDayOfMonth()) +
                             "/hour="   +  "%02d".format(now.getHour()) + 
                             "/minute=" +  "%02d".format(now.getMinute())
    
    tableDf.write.format(backupFormat).mode(SaveMode.ErrorIfExists).save(fullbackuplocation)

    Job.commit()
  }

  
  
}
