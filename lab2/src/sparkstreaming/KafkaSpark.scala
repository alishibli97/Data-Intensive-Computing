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
    
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,
      kafkaConf,
      Set("avg")
    )

    val value = messages.map{case (key,value)=>value.split(",")}
    val pairs = value.map(record => (record(1), record(2)))
    print(pairs)

    // print(messages.map(case (key,value)=>value.split(","))

    // <FILL IN>

    // measure the average value for each key in a stateful manner
    // def mapWithState[StateType, MappedType](spec: StateSpec[K, V, StateType, MappedType]): DStream[MappedType]

    // val mappingFunc = (key: String, value: Option[Double], state: State[Double]) => { // : (String, Double)
    //   // return (key,value.getOrElse(0))
    //   state.update(1.0)
    //   ("hey",2.0)
    // }
    // val pairs = messages.
    // val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc)) //<FILL IN>)
    
    ssc.start()
    ssc.awaitTermination()
  }
}
