package sjtu.edu.cn

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

object GraphX {

  def main(args: Array[String]) {

    // 设置运行环境及master节点
    val conf = new SparkConf().setAppName("Simple GraphX").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val nodes = NeoData.nodes
    val relationships = NeoData.relationships

    // Not Serializable, can not use Node & Relationship
    var vertexArray: List[(Long, Map[String, Object])] = List()
    var edgeArray: List[Edge[Map[String, Object]]] = List()

    for (node <- nodes) {
      vertexArray :+= (node.id(), node.asMap().toMap)
    }

    for (relationship <- relationships) {
      edgeArray :+= Edge(relationship.startNodeId(), relationship.endNodeId(), relationship.asMap().toMap)
    }

    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, Map[String, Object])] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Map[String, Object]]] = sc.parallelize(edgeArray)

    vertexRDD.foreach(v => {
      println(v._1 + ", " + v._2("id") + ", " + classOf[String].cast(v._2("name")))
    })

    edgeRDD.foreach(v => {
      println(v.srcId + ", " + v.dstId + ", " + classOf[String].cast(v.attr("date")) + ", " + v.attr("amount"))
    })

    val graph: Graph[Map[String, Object], Map[String, Object]] = Graph(vertexRDD, edgeRDD)

    // Compute the max degrees
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    val maxInDegree: (VertexId, Int) = graph.inDegrees.reduce(max)
    val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
    val maxDegrees: (VertexId, Int) = graph.degrees.reduce(max)

    println("Max of InDegrees: " + maxInDegree)
    println("Max of OutDegrees: " + maxOutDegree)
    println("Max of Degrees: " + maxDegrees)

    // 排序，白名单：vertexId=XXX, 获取前n名的点
    println("Sort with 'vertexId' in 'white list', get TOP N: ")
    val whiteList = List(65385, 65364)
    val n = 3
    val topNVertex = graph.degrees.sortBy(each => each._2, false).filter(each => !whiteList.contains(each._1.intValue())) take n
    topNVertex.foreach(each => println("ID: " + each._1 + ", degrees: " + each._2))

    println()

    // 只要有一个顶点在topNVertex中，即：从topNVertex顶点出发，长度为1的边集
    println("Final Edge RDD: ")
    val finalEdgeRDD = edgeRDD.filter(each => topNVertex.exists(_._1 == each.srcId) || topNVertex.exists(_._1 == each.dstId))
    var finalVertexId: Set[Long] = Set()
    finalEdgeRDD.collect().foreach(v => {
      finalVertexId += v.srcId
      finalVertexId += v.dstId
      println(v.srcId + ", " + v.dstId + ", " + classOf[String].cast(v.attr("date")) + ", " + v.attr("amount"))
    })

    print("Final VertexId: ")
    finalVertexId.foreach(each => print(each + " "))
    println()

    println("Final Vertex RDD: ")
    val finalVertexRDD = vertexRDD.filter(each => finalVertexId.contains(each._1))
    finalVertexRDD.foreach(v => {
      println(v._1 + ", " + v._2("id") + ", " + classOf[String].cast(v._2("name")))
    })

    // for output
    val finalGraph: Graph[Map[String, Object], Map[String, Object]] = Graph(finalVertexRDD, finalEdgeRDD)

  }
}
