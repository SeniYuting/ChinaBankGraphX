package sjtu.edu.cn

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphX {
  def main(args: Array[String]) {
    // 设置运行环境及master节点
    val conf = new SparkConf().setAppName("Simple GraphX").setMaster("local")
    val sc = new SparkContext(conf)

    // 构造图
    // 顶点
    val vertexArray = Array(
      (1L, ("Alice", 38)),
      (2L, ("Henry", 27)),
      (3L, ("Charlie", 55)),
      (4L, ("Peter", 32)),
      (5L, ("Mike", 35)),
      (6L, ("Kate", 23))
    )

    // 边
    val edgeArray = Array(
      Edge(2L, 1L, 5),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 7),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 3),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 8)
    )

    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    // 构造图
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    // Degree操作
    println("Max OutDegrees, InDegrees and Degrees:")

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    // Compute the max degrees
    val maxInDegree: (VertexId, Int) = graph.inDegrees.reduce(max)
    val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
    val maxDegrees: (VertexId, Int) = graph.degrees.reduce(max)

    println("Max of InDegrees:" + maxInDegree)
    println("Max of OutDegrees:" + maxOutDegree)
    println("Max of Degrees:" + maxDegrees)

    // 排序，白名单：vertexId=3
    println("Sort with white list vertexId = 3:")
    val sortedVertex = graph.degrees.sortBy(each => each._2, false).filter(each => each._1.intValue() != 3)
    sortedVertex.foreach(each => println("ID: " + each._1 + ", degrees: " + each._2))
  }
}
