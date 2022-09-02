import SparkUtils.{getSQLContext, getSparkContext, getStreamingContext}
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import com.datastax.spark.connector.streaming._
import org.apache.spark.rdd.RDD
import org.json.JSONObject

import scala.util.Try


object RealTime {

  def rddToRDDActivity(input: RDD[(String, String)]): RDD[Activity] = {
    input.mapPartitionsWithIndex( {(_, it) =>
      it.flatMap { A =>
        val line = A._2
        val JSONObject = new JSONObject(line)
//        println(
//          "\n\n"+
//          JSONObject.getString("course_id") + "\n" +
//          JSONObject.getString("user_id") + "\n" +
//          JSONObject.getString("session_id") + "\n" +
//          JSONObject.getString("activity_event") + "\n" +
//          JSONObject.getString("time")
//        )
        Some(
          Activity(
            JSONObject.getString("course_id"),
            JSONObject.getString("user_id"),
            JSONObject.getString("session_id"),
            JSONObject.getString("activity_event"),
            JSONObject.getString("time")
          )
        )
      }
    })
  }

  def main(args: Array[String]): Unit = {
    // setup spark context
    val sc = getSparkContext("F&M_Project")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._

    val batchDuration = Seconds(4)


    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)
//      val wlc = Settings.WebLogGen
      val topic = "topicA"
      println(topic)

      val kafkaDirectParams = Map(
        "metadata.broker.list" -> "localhost:9092",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "largest"
      )

      var fromOffsets : Map[TopicAndPartition, Long] = Map.empty
      val hdfsPath = "/home/mohammadreza/Documents/Hadoop"

      Try(sqlContext.read.parquet(hdfsPath)).foreach( hdfsData =>
        fromOffsets = hdfsData.groupBy("topic", "kafkaPartition").agg(max("untilOffset").as("untilOffset"))
          .collect().map { row =>
          (TopicAndPartition(row.getAs[String]("topic"), row.getAs[Int]("kafkaPartition")), row.getAs[String]("untilOffset").toLong + 1)
        }.toMap
      )

      val kafkaDirectStream = fromOffsets.isEmpty match {
        case true =>
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc, kafkaDirectParams, Set(topic)
          )
        case false =>
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
            ssc, kafkaDirectParams, fromOffsets, { mmd: MessageAndMetadata[String, String] => (mmd.key(), mmd.message()) }
          )
      }

      val activityStream = kafkaDirectStream.transform(input => {
        rddToRDDActivity(input)
      }).cache()

//       save data to HDFS
      activityStream.foreachRDD { rdd =>
        val activityDF = rdd
          .toDF()
          .selectExpr("course_id", "user_id", "session_id", "activity_event", "time"
          )

        activityDF
          .write
          .partitionBy("time")
//          .mode(SaveMode.Append)
//          .parquet(hdfsPath)
      }

      activityStream.saveToCassandra("lambda", "activity")
      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)
    //ssc.remember(Minutes(5))
    ssc.start()
    ssc.awaitTermination()

  }

}
