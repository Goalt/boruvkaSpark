package com.wrapper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import scala.io.Source
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.graphframes.GraphFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

object BoruvkaAlgorithm {
  val sparkSession = SparkSession.builder().appName("boruvka").getOrCreate()
  val sparkContext = sparkSession.sparkContext
  // sparkSession.conf.set("spark.sql.shuffle.partitions", 4)
  sparkContext.setLogLevel("ERROR")
  sparkContext.setCheckpointDir("tmp/")
  import sparkSession.implicits._

  /** Function for reading graphframe from the file
    File with graph should have this pattern:
    <src_id>: <dst_id> (<weight>), ..., <dst_id> (<weight>),
    ...
    <src_id>: <dst_id> (<weight>), ..., <dst_id> (<weight>),
    Args:
        filePath (String): The path to the file with graph.
    Returns:
        GraphFrame: The graph.
  */
  def readGraphFrameFromFile(filePath: String) : GraphFrame = {
    var edges = Array[(Long, Long, Double)]()
    var vertices = Set[Long]()
    
    for (line <- Source.fromFile(filePath).getLines) {
      val splt = line.split(":")
      var srcId = splt(0).toLong
      vertices += srcId

      if (splt.length == 2) {
        var rightPartSplt = splt(1).replace(")", "").replace("(", "").split(", ")

        for (i <- rightPartSplt) {
            var tmp = i
            var dstId = tmp.split(" ")(0).toLong
            var weight = tmp.split(" ")(1).toDouble
            edges = edges :+ ((srcId, dstId, weight))
        }
      }
      else {
        edges = edges :+ ((srcId, srcId, 0.1))
      }
    }

    val schemaEdges = StructType(
      List(
        StructField("src", LongType, true),
        StructField("dst", LongType, true),
        StructField("weight", DoubleType, true)
      )
    )
    val edgesRDD = sparkContext.parallelize(edges).map(x => Row(x._1, x._2, x._3))
    val edgesDF = sparkSession.createDataFrame(edgesRDD, schemaEdges).withColumn("UniqueID", monotonically_increasing_id)

    val schemaVertex = StructType(
      List(
        StructField("id", LongType, true)
      )
    )
    val verticesList = vertices.toArray
    val vertexRDD = sparkContext.parallelize(verticesList).map(x => Row(x))
    val vertexDF = sparkSession.createDataFrame(vertexRDD, schemaVertex)

    return GraphFrame(vertexDF, edgesDF)
  }

  /** Function for deleting edges where srcId >= dstID
  Args:
      graph (GraphFrame): Directed graph.
  Returns:
      GraphFrame: The undirected graph.
  */
  def convertGraphToUndirected(graph: GraphFrame) : GraphFrame = {
    var copyEdges = graph.edges
    copyEdges = copyEdges.filter("src < dst")
    return GraphFrame(graph.vertices, copyEdges)
  }

  /** Function for finding minimum edge for each vertex
  Args:
      edges (DataFrame): Edges.
  Returns:
      DataFrame: The minimum edges.
  */
  def findMinEdge(edges: DataFrame) : DataFrame = {
    val cm = new CustomMin
    var minEdges = edges.groupBy("src").agg(cm(edges.col("dst"), edges.col("weight"), edges.col("UniqueID")).as("minArray"))
    minEdges = minEdges.select($"src", $"minArray".getItem(0).as("dstDouble"), $"minArray".getItem(1).as("weight"), $"minArray".getItem(2).as("UniqueIDDouble")).drop("minArray")
    return minEdges.withColumn("dst", minEdges.col("dstDouble").cast(LongType)).withColumn("UniqueID", minEdges.col("UniqueIDDouble").cast(LongType)).drop("dstDouble", "UniqueIDDouble")
  }

  /** Function for sum weight of edges
  Args:
      graph (GraphFrame): Directed graph.
  Returns:
      Double: The sum
  */
  def sumWeight(graph: GraphFrame) : Double = {
    return graph.edges.select(col("weight")).rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)
  }

  def findEdgeWithMaxWeight = udf((uid1: Long, weight1: Double, uid2: Long, weight2: Double) => {
    if(weight1 < weight2) uid2
    else uid1
  } )

  /** Function for buildingMST
  Args:
      v (DataFrame): vertices
      e (DataFrame): edges
  Returns:
      graph (GraphFrame): MST
  */
  def runAlgorithm(v: DataFrame, e: DataFrame) : GraphFrame = {
    val schema = StructType(StructField("UniqueID", LongType, false) :: Nil)
    var MSTDF = sparkSession.createDataFrame(sparkContext.emptyRDD[Row], schema)
    var copyEdges = e
    var copyVert = v
    var prevNumberUnique: Long = -1
    var numberUnique: Long = 0
    copyEdges = copyEdges.filter("src != dst")

    while (numberUnique != prevNumberUnique) {
      val s = System.nanoTime
      prevNumberUnique = numberUnique

      // Finding min edges
      var minEdges = findMinEdge(copyEdges).cache()
      minEdges.count()

      // Making new graph with min edges
      var tmpGraph = GraphFrame(copyVert, minEdges)

      // Find connected components
      var components = tmpGraph.connectedComponents.setCheckpointInterval(1).run().cache()
      components.count()

      // Finding cycles
      var cycles = tmpGraph.find("(a)-[e]->(b); (b)-[e2]->(a)").filter("a < b")
      cycles = cycles.select($"e.src", $"e.dst", $"e.UniqueID".as("uid1"), $"e.weight".as("weight1"), $"e2.UniqueID".as("uid2"), $"e2.weight".as("weight2"))
      cycles = cycles.withColumn("uidToDelete", findEdgeWithMaxWeight(cycles("uid1"), cycles("weight1"), cycles("uid2"), cycles("weight2")))
      cycles = cycles.select("uidToDelete")

      // Deleting cycles (edges with bigger weight) from min edges
      var noCycles = minEdges.join(cycles, minEdges("UniqueID") === cycles("uidToDelete"), "left_outer").filter($"uidToDelete".isNull).drop("uidToDelete")

      // Add UniqueID of min edges to MST
      MSTDF = MSTDF.union(noCycles.select("UniqueID")).cache()
      MSTDF.count()

      // New vertices
      copyVert = components.select($"component".as("id")).distinct().cache()
      numberUnique = copyVert.count()
      
      // Relabel edges
      copyEdges = copyEdges.join(components, copyEdges("src") === components("id"), "left_outer").select($"component".as("src"), $"dst", $"weight", $"UniqueID")
      copyEdges = copyEdges.join(components, copyEdges("dst") === components("id"), "left_outer").select($"src", $"component".as("dst"), $"weight", $"UniqueID")
      copyEdges = copyEdges.filter("src != dst").cache()
      copyEdges.count()
      println("Time per Iteration:")
      println((System.nanoTime-s)/1e6)
    }

    MSTDF = MSTDF.withColumnRenamed("UniqueID", "uid")
    var resultEdges = MSTDF.join(e, MSTDF("uid") === e("UniqueID")).select("src", "dst", "weight", "UniqueID")
    return GraphFrame(v, resultEdges)
  }

  def convertToParquet(path: String) = {
    val g = readGraphFrameFromFile(path)
    g.vertices.write.parquet("vertices")
    g.edges.write.parquet("edges")
  }

  def test(filePath: String) = {
    // convertToParquet(filePath)
    println("Reading:")
    var s = System.nanoTime
    val v = sparkSession.read.parquet(filePath + "/vertices")
    val e = sparkSession.read.parquet(filePath + "/edges")
    println((System.nanoTime-s)/1e6)
    println("--------------------------")

    s = System.nanoTime
    println("Edges Count:")
    println(e.count())
    println("Vertices Count:")
    println(v.count())
    println("Time:")
    println((System.nanoTime-s)/1e6)
    println("--------------------------")

    println("Running:")
    s = System.nanoTime
    val res = runAlgorithm(v, e)
    println(sumWeight(res))
    println("Time:")
    println((System.nanoTime - s) / 1e6)
    println("--------------------------")
  }

  def main(args: Array[String]): Unit = {
    println(args(0))
    test(args(0))
  }
}