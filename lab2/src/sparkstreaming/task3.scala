import org.apache.spark.graphx._

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

def nameof(e: Long) : String = {
    myGraph.vertices.filter{
        id => if () println(id._2._1)// else ("") // id._1._1 == 4L
    }
}

def nameK(e: Edge) {
    myGraph.vertices.foreach{
        id => println(id._2._1)
    }
}

def nameKK(e: Edge) {
    myGraph.edges.foreach{
        id => println(id.getClass)
    }
}

myGraph.vertices.collect

// 1
myGraph.vertices.distinct.foreach{ e => if (e._2._2 > 30) print(e) }

// 2
myGraph.edges.distinct.foreach{ e => println(nameof(e.srcId), " likes ", nameof(e.dstId)) }
