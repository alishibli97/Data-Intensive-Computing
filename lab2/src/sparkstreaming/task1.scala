package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import org.apache.spark.sql.functions.{min, max, avg, desc, udf, col, explode, count, split}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, StreamingQueryException}
import org.apache.spark.sql._
import java.sql.Timestamp

case class Call (val key: String, val value: Double)

object KafkaSpark {
  def main(args: Array[String]) {   

    val spark = SparkSession
      .builder
      .appName("lab2")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val checkpointDir = "checkpoints/."

    // task 1
    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = Map(
        "metadata.broker.list" -> "localhost:9092",
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "kafka-spark-streaming",
        "zookeeper.connection.timeout.ms" -> "1000")
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint(checkpointDir)
    
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,
      kafkaConf,
      Set("avg")
    )

    val value = messages.map{case (key,value)=>value.split(",")}
    val pairs = value.map(record => (record(0), record(1).toDouble))

    def mappingFunc(key: String, value: Option[Double], state: State[(Double,Int)]): (String, Double) = { // Map[String,(Double,Int)]
      val newSum = value.getOrElse(0.0) + state.getOption.getOrElse(0.0,0)._1 // map_avgs(key)._1 * map_avgs(key)._2
      val newCnt = state.getOption.getOrElse((0.0,0))._2 + 1
      state.update(newSum, newCnt)
      val new_avg = newSum/newCnt
      (key, new_avg)
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _)) //<FILL IN>)
    stateDstream.print()
    // task 1

    ssc.start()
    ssc.awaitTermination()
  }
}
