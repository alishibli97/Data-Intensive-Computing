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

object KafkaSpark {
  def main(args: Array[String]) {
    // make a connection to Kafka and read (key, value) pairs from it
    // <FILL IN>
    val kafkaConf = Map(
          "metadata.broker.list" -> "localhost:9092",
          "zookeeper.connect" -> "localhost:2181",
          "group.id" -> "kafka-spark-streaming",
          "zookeeper.connection.timeout.ms" -> "1000")
    
    // val spark = SparkSession.builder().getOrCreate().config("spark.master", "local")
    val spark = SparkSession
      .builder()
      .appName("lab2")
      .config("spark.master", "local")
      .getOrCreate()

    // val conf = new SparkConf().setAppName("lab2")//.setMaster("local[2]")
    // val ssc = new StreamingContext(conf, Seconds(1))
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))
    val checkpointDir = "checkpoints/."
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

    // def keyFunc(key: String){
    //   ("a")
    // }

    // task 1
    // val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _)) //<FILL IN>)
    // stateDstream.print()
    // task 1

    // task 2
    val inputDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "avg") // subscribe to topic "avg"
      .load()

    // inputDF.printSchema()
    val df = inputDF.selectExpr("CAST(value AS STRING)").select(split(col("value"),",").alias("value"))
    // df.printSchema()
    df.printSchema()

    val query = df.writeStream.outputMode("append").format("console").start().awaitTermination()

    // df.groupByKey(keyFunc) // keyFunction() generates key from input
    // .mapGroupsWithState(mappingFunc)

    // def mappingFunc2(key: String, value: Iterator[Row], state: GroupState[(Double,Int)]): (String, Double) = { // , 
    //   if (value.isEmpty && state.hasTimedOut) {
    //     ("a", 0.0)
    //   } else {
    //     state.setTimeoutTimestamp(2000)
    //     // val stateNames = state.getOption.getOrElse(Seq.empty)

    //     val new_key = value.next().getString(0).split(",")(0)
    //     val new_val = value.next().getString(0).split(",")(1).toInt
    //     val newSum = new_val + state.getOption.getOrElse(0.0,0)._1
    //     val newCnt = state.getOption.getOrElse((0.0,0))._2 + 1

    //     state.update(newSum, newCnt)
    //     val new_avg = newSum/newCnt
    //     (key, new_avg)
    //   }

    // }



    // import spark.implicits._
    // val mappedValues = df
    //   .groupByKey(row => row.getString(0))
    //   .mapGroupsWithState(timeoutConf = GroupStateTimeout.EventTimeTimeout())(mappingFunc2)

    // val query = df.writeStream
    //   .format("console")
    //   .outputMode("update") //complete
    //   .start()
      //.awaitTermination()


    //////////

    // inputDF.show()

    // val avgDf = inputDF.groupBy("value").count().withColumn("avg",inputDF("count")/inputDF("value"))

    // import sqlContext.implicits._
    // avgDf.foreachRDD(rdd => {
    //   val avg = rdd.reduce(_+_)/rdd.count()
    //   println(s"Average value of key is $avg")
    // })

    // task 2

    ssc.start()
    ssc.awaitTermination()
  }
}
