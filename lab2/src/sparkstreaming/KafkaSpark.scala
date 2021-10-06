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
// import org.apache.spark.SparkContext

object KafkaSpark {
  def main(args: Array[String]) {
    // make a connection to Kafka and read (key, value) pairs from it
    // <FILL IN>
    val kafkaConf = Map(
          "metadata.broker.list" -> "localhost:9092",
          "zookeeper.connect" -> "localhost:2181",
          "group.id" -> "kafka-spark-streaming",
          "zookeeper.connection.timeout.ms" -> "1000")
    
    val conf = new SparkConf().setAppName("lab2").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("checkpoints/.")
    
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,
      kafkaConf,
      Set("avg")
    )

    val value = messages.map{case (key,value)=>value.split(",")}
    val pairs = value.map(record => (record(0), record(1).toDouble))

    // val pairs = value.map(record => (record(1), record(2).toDouble))

    // // github
    // def mappingFunc(key: String, value: Option[Double], state: State[Double]): Option[(String, Double)] = { // (String, Double)
    //     val sum = value.getOrElse(0.0) + state.getOption.getOrElse(0.0)
    //     val output = Option(key, sum)
    //     state.update(sum)
    //     output
    // }
    // val spec = StateSpec.function(mappingFunc _)
    // val stateDstream = pairs.mapWithState(spec)

    // // store the result in Cassandra
    // stateDstream.print()
    // github

    // print(messages.map(case (key,value)=>value.split(","))

    // <FILL IN>

    // measure the average value for each key in a stateful manner
    // def mapWithState[StateType, MappedType](spec: StateSpec[K, V, StateType, MappedType]): DStream[MappedType]

    def mappingFunc(key: String, value: Option[Double], state: State[Map[String,Tuple(Double,Int)]]): (String, Double) = { // State[(Map[key,Tuple(Double,Int)])]
      // val (currentkey, sum, cnt) = state.getOption.getOrElse(("a", 0.0, 0))
      val map_avgs = state.getOption.getOrElse((Map()))
      val newSum = value.getOrElse(0.0) + map_avgs(key)(0) * map_avgs(key)(1)
      val newCnt = cnt + 1
      // state.update((key, newSum, newCnt))
      new_avg = newSum/newCnt
      map_avgs(key) = (new_avg, newCnt)
      state.update(map_avgs)
      (key, new_avg)
    }

    // def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
	  // <FILL IN>
    // }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _)) //<FILL IN>)
    stateDstream.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}
