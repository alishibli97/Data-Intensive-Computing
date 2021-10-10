package sparkstreaming

import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession


object KafkaSpark {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("lab2")
      .config("spark.master", "local")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    val myVertices = sc.makeRDD(Array(
        (1L, ("Alice", 28)), 
        (2L, ("Bob", 27)), 
        (3L, ("Charlie", 65)), 
        (4L, ("David", 42)), 
        (5L, ("Ed", 55)), 
        (6L, ("Fran", 50)), 
        (7L, ("Alex", 55))))

    val myEdges = sc.makeRDD(Array(
        Edge(2L, 1L, 7),
        Edge(4L, 1L, 1),
        Edge(2L, 4L, 2),
        Edge(3L, 2L, 4),
        Edge(5L, 2L, 2),
        Edge(5L, 3L, 8),
        Edge(3L, 6L, 3),
        Edge(5L, 6L, 3),
        Edge(7L, 5L, 3),
        Edge(7L, 6L, 4)
        ))

    val myGraph = Graph(myVertices, myEdges)

    def nameof(target_id: Long): String = {
        myGraph.vertices.collect.filter{ case (id, (name, pos)) => id == target_id}(0)._2._1
    }

    def getCount(id: Long): Long = {
        myGraph.edges.filter{e => e.dstId==id}.count
    }

    def outgoing(id: Long): Long = {
        myGraph.edges.filter{e=>e.srcId==id}.count
    }

    def ingoing(id: Long): Long = {
        myGraph.edges.filter{e=>e.dstId==id}.count
    }

    // println(myGraph.vertices.collect)

    // 1
    // myGraph.vertices.foreach{ e => if (e._2._2 > 30) println(e) }

    // // 2
    // myGraph.edges.collect.foreach{e => println(nameof(e.srcId)+" likes "+nameof(e.dstId))}

    // // 3
    // myGraph.edges.collect.foreach{e => if(e.attr>5) println(nameof(e.srcId)+" loves "+nameof(e.dstId))}

    // // 4
    // myGraph.vertices.collect.foreach{v => println(v._2._1+" is liked by "+getCount(v._1))}

    // // 5
    // myGraph.vertices.collect.foreach{v => if(outgoing(v._1)==ingoing(v._1)) println(v._2._1)}

    // // 6
    // case class User(name: String, age: Int)
    // val usergraph: Graph[User, Int] = myGraph.mapVertices{ case (id, (name, age)) => User(name, age) }
    // val result = usergraph.aggregateMessages[(String, Int)](
    // // sendMsg
    // triplet => triplet.sendToDst(triplet.srcAttr.name, triplet.srcAttr.age),
    // // mergeMsg
    // (a, b) => (if (a._2 > b._2) a else b)
    // )

    // // for(res <- result) println(res)

    // usergraph.vertices.leftJoin(result) { (id, user, oldestFollower) =>
    // oldestFollower match {
    //     case None => user.name + " does not have any followers"
    //     case Some((name, age)) => name + " is the oldest follower of " + user.name
    // }
    // }.collect.foreach { case (id, str) => println(str) }

    // spark.stop()
  }
}
