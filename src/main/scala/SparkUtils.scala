import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.lang.management.ManagementFactory

object SparkUtils {
  def getSparkContext(appName: String) = {
    val checkpointDirectory = "/home/mohammadreza/Documents/Check"

    // get spark configuration
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.casandra.connection.host", "192.168.0.3")
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
      .setMaster("local[*]")

//    System.setProperty("hadoop.home.dir", "F:\\Libraries\\WinUtils")


    // setup spark context
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkpointDirectory)
    sc
  }

  def getSQLContext(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

  def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext, sc : SparkContext, batchDuration: Duration) = {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach( cp => ssc.checkpoint(cp))
    ssc
  }

}
