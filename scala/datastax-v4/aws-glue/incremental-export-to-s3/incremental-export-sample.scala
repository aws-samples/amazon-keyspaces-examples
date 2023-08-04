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
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql._
import com.datastax.oss.driver.api.core._
    
import org.apache.spark.sql.functions.rand

import com.amazonaws.services.glue.log.GlueLogger

object GlueApp {

  def main(sysArgs: Array[String]) {

  val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "KEYSPACE_NAME", "TABLE_NAME", "DRIVER_CONF", "FORMAT", "S3_URI","DISTINCT_KEYS", "TODAY_DATE", "YESTERDAY_DATE").toArray)

  val driverConfFileName = args("DRIVER_CONF")

  val conf = new SparkConf()
      .setAll(
       Seq(
           ("spark.task.maxFailures",  "10"),
          
          ("spark.cassandra.connection.config.profile.path",  driverConfFileName),
          ("spark.cassandra.query.retry.count", "1000"),

          ("spark.cassandra.sql.inClauseToJoinConversionThreshold", "0"),
          ("spark.cassandra.sql.inClauseToFullScanConversionThreshold", "0"),
          ("spark.cassandra.concurrent.reads", "512"),

          ("spark.cassandra.output.concurrent.writes", "8"),
          ("spark.cassandra.output.batch.grouping.key", "none"),
          ("spark.cassandra.output.batch.size.rows", "1")
      ))

    val spark: SparkContext = new SparkContext(conf)
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession

    import sparkSession.implicits._

    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    
    val logger = new GlueLogger
    
    val backupFormat = args("FORMAT")
    val s3bucketBackupsLocation = args("S3_URI")
    val todayString = args("TODAY_DATE")
    val yesterdayString = args("YESTERDAY_DATE")
    val distinctKeys = args("DISTINCT_KEYS").filterNot(_.isWhitespace).split(",")
    
    logger.info("distinctKeys: " + distinctKeys.mkString(", "))

    val today = sparkSession.read.format(backupFormat).load(s3bucketBackupsLocation + '/' + todayString + "/full-snapshot/")    
      
    val yesterday = sparkSession.read.format(backupFormat).load(s3bucketBackupsLocation + '/' + yesterdayString + "/full-snapshot") 
    
    import uk.co.gresearch.spark.diff._
    
    val changeSinceYesterday =  yesterday.diff(today, distinctKeys:_*).filter($"diff" =!= "N")
    
    changeSinceYesterday.repartition(1).write.format(backupFormat).mode(SaveMode.ErrorIfExists).save(s3bucketBackupsLocation + '/' + todayString + "/incremental-snapshot/")
    
    // I is INSERT
    // D is DELETE 
    // C is UPDATE Both previous and new values are prefixed with left_ and right_.
    // N is Not changed. We can filter it out to reduce data size. 
    // Below Sample
    
    //{"diff":"I","id":"-67214788834858333A","create_date":"2023-08-01T18:23:59.476Z","right_count":1,"right_data":"-67214788834858333A"}
    //{"diff":"D","id":"-7134365464711695755","create_date":"2023-02-25T19:08:26.000Z","left_count":-823388298404028328,"left_data":"-7444764033894103459"}
    //{"diff":"C","id":"-672147888348583332","create_date":"2023-02-27T20:04:30.000Z","left_count":-2365493309516205255,"right_count":1,"left_data":"-1643515377232023460","right_data":"-1643515377232023460"}
    //{"diff":"I","id":"4053134097057361633","create_date":"2023-08-01T18:28:34.454Z","right_count":1,"right_data":"4053134097057361633"}
    
   Job.commit()
  }
}
