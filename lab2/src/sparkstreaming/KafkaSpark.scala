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
    
    // val sc = new SparkContext("local", "mapWithState", new SparkConf())
    val conf = new SparkConf().setAppName("lab2")
    val ssc = new StreamingContext(conf, Seconds(1))
    
    val messages = KafkaUtils.createDirectStream[String,Double,StringDecoder,DefaultDecoder](
      ssc,
      kafkaConf,
      Set("avg")
    )

    // <FILL IN>

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
      return (key,value.getOrElse(0))
    }

    println(messages)
    // val stateDstream = messages.mapWithState(StateSpec.function(mappingFunc) //<FILL IN>)

    // def mapWithState[StateType, MappedType](spec: StateSpec[K, V, StateType, MappedType]): DStream[MappedType]


    ssc.start()
    ssc.awaitTermination()
  }
}
