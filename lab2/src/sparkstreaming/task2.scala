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

    import spark.implicits._

    val checkpointDir = "checkpoints/."

    // subscribe to topic
    import spark.implicits._
    val inputDF = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092") // host1:port1,host2:port2
        .option("subscribe", "avg")
        .load()

    spark.sparkContext.setLogLevel("ERROR")

    // val ds = inputDF.selectExpr("CAST(value AS STRING)")
    //     .withColumn("_tmp", split($"value", ",")).select(
    //         $"_tmp".getItem(0).as("key"),
    //         $"_tmp".getItem(1).toDouble.as("value")
    //     )//.groupBy("key")
    // ds.printSchema()

    val ds = inputDF
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]
        .map(x => x._2.split(","))
        .map(x => (x(0), x(1).toDouble))
        .withColumnRenamed("_1", "key")
        .withColumnRenamed("_2", "value")

    val df_obj = ds.as[Call]

    // A mapping function that maintains an integer state for string keys and returns a string.
    // Additionally, it sets a timeout to remove the state if it has not received data for an hour.
    def mappingFunction(key: String, value: Iterator[Call], state: GroupState[(Double,Int)]): (String, Double) = {

        println(key)

        var newSum = 0.0
        var newCnt = 0

        if (state.hasTimedOut) {                // If called when timing out, remove the state
            state.remove()

        } else if (state.exists) {              // If state exists, use it for processing
            val existingState = state.get         // Get the existing state

            // newSum = value.next().toDouble + state.getOption.getOrElse(0.0,0)._1
            newSum = value.next().value.toDouble + state.getOption.getOrElse(0.0,0)._1
            newCnt = state.getOption.getOrElse((0.0,0))._2 + 1


            // val newState = ...
            // state.update(newState)              // Set the new state
            state.update(newSum,newCnt)
            state.setTimeoutDuration("1 hour")  // Set the timeout

            val new_avg = newSum/newCnt
            return (key, new_avg)

        } else {
            // val initialState = ...
            // state.update(initialState)            // Set the initial state
            state.update(0.0,0)
            state.setTimeoutDuration("1 hour")    // Set the timeout
        }

        var new_avg = 0.0

        if (newCnt != 0){
            new_avg = newSum/newCnt
        }
        
        return (key, new_avg)

    }

    // Method1: use built in avg function
    // val result = df_obj.groupBy("key").avg("value")
    
    // Method2: groupby key then apply custom function mappingFunction
    val result = df_obj
        .groupByKey(event => event.key) //(event => event.col1)
        .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(mappingFunction _)

    // Writing to console:
    result.writeStream
        .format("console")
        .outputMode("update")
        .start()
        .awaitTermination()

    // Writing to kafka:
    // val streamingQuery = result
    //     .selectExpr("cast(key as string) as key", "cast(value as string) as value")
    //     .writeStream
    //     .format("kafka")
    //     .option("kafka.bootstrap.servers", "localhost:9092")
    //     .option("topic", "avg")
    //     .outputMode("update")
    //     .option("checkpointLocation", checkpointDir)
    //     .start()
    //     .awaitTermination()

  }
}
