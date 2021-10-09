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

case class Call (val key: String, val value: String)

object KafkaSpark {
  def main(args: Array[String]) {   
    // val spark = SparkSession.builder().getOrCreate().config("spark.master", "local")
    val spark = SparkSession
      .builder
      .appName("lab2")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val checkpointDir = "checkpoints/."

    // task 1
    // make a connection to Kafka and read (key, value) pairs from it
    // val kafkaConf = Map(
    //     "metadata.broker.list" -> "localhost:9092",
    //     "zookeeper.connect" -> "localhost:2181",
    //     "group.id" -> "kafka-spark-streaming",
    //     "zookeeper.connection.timeout.ms" -> "1000")
    // val sc = spark.sparkContext
    // val ssc = new StreamingContext(sc, Seconds(1))
    // ssc.checkpoint(checkpointDir)
    
    // val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
    //   ssc,
    //   kafkaConf,
    //   Set("avg")
    // )

    // val value = messages.map{case (key,value)=>value.split(",")}
    // val pairs = value.map(record => (record(0), record(1).toDouble))

    // def mappingFunc(key: String, value: Option[Double], state: State[(Double,Int)]): (String, Double) = { // Map[String,(Double,Int)]
    //   val newSum = value.getOrElse(0.0) + state.getOption.getOrElse(0.0,0)._1 // map_avgs(key)._1 * map_avgs(key)._2
    //   val newCnt = state.getOption.getOrElse((0.0,0))._2 + 1
    //   state.update(newSum, newCnt)
    //   val new_avg = newSum/newCnt
    //   (key, new_avg)
    // }

    // val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _)) //<FILL IN>)
    // stateDstream.print()
    // task 1

    // task 2
    // import spark.implicits._
    val inputDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "avg") // subscribe to topic "avg"
      .load()


    inputDF.printSchema()
    val df = inputDF.selectExpr("CAST(value AS STRING)") //.select(split(col("value"),",").select(
        .withColumn("_tmp", split($"value", ",")).select(
        $"_tmp".getItem(0).as("key"),
        $"_tmp".getItem(1).as("value")
        )
    // println(df.columns.size)
    // df.printSchema()
    // UP TO HERE OK.
    
    val ds = df.as[Call]
    val results = ds.groupBy("value").count()

    val query = results.writeStream
        .outputMode("complete")
        .format("console")
        .start()
    query.awaitTermination()


    // ds.printSchema()
    // ds.show()

     // A mapping function that maintains an integer state for string keys and returns a string.
    // Additionally, it sets a timeout to remove the state if it has not received data for an hour.
    def mappingFunction(key: String, value: Iterator[Call], state: GroupState[(Double,Int)]): (String, Double) = {

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
        // ...

        var new_avg = 0.0

        if (newCnt != 0){
            new_avg = newSum/newCnt
        }
        
        return (key, new_avg)
        // return something

    }

    // import spark.implicits._
    // ds
    //     .groupByKey(event => event.key) //(event => event.col1)
    //     .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(mappingFunction _)


    // BELOW ALSO OK

    // val query = inputDF.writeStream.outputMode("append").format("console").start()
    // query.awaitTermination()



    // val streamingQuery = ds
    //     .selectExpr("cast(key as string) as key", "cast(value as string) as value")
    //     .writeStream
    //     .format("kafka")
    //     .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    //     .option("topic", "avg")
    //     .outputMode("update")
    //     .option("checkpointLocation", checkpointDir)
    //     .start()


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

    // val query = sessionUpdates
    //   .writeStream
    //   .outputMode("update")
    //   .format("console")
    //   .start()


    // ssc.start()
    // ssc.awaitTermination()
  }
}
