package final_project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MaxMatchDriver {

  // A simple greedy matching algorithm
  def greedyMatch(graph: Graph[Int, Int]): RDD[(VertexId, VertexId)] = {
    val edges = graph.edges.map(e => (e.srcId, e.dstId)).cache()
    val matchedVertices = scala.collection.mutable.Set[VertexId]()
    val matchedEdges = scala.collection.mutable.ListBuffer[(VertexId, VertexId)]()

    println("Starting matching loop...")

    edges.collect().foreach { case (src, dst) =>
      if (!matchedVertices.contains(src) && !matchedVertices.contains(dst)) {
        matchedVertices += src
        matchedVertices += dst
        matchedEdges += ((src, dst))
      }
    }

    println(s"Finished matching loop. Total matched edges: ${matchedEdges.size}")

    // Create an RDD from matched edges
    graph.edges.sparkContext.parallelize(matchedEdges)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("Usage: final_project.MaxMatchDriver compute <input_graph.csv> <output_dir>")
      sys.exit(1)
    }

    val Array(_, inputPath, outputPath) = args

    val conf = new SparkConf()
      .setAppName("GreedyMaxMatching")
      .setMaster("local[*]") // Ensure it works locally
      .set("spark.driver.memory", "4g") 
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    println(s"Reading input graph from $inputPath...")

    val startTimeMillis = System.currentTimeMillis()
    
    val rawEdges: RDD[Edge[Int]] = sc.textFile(inputPath).map { line =>
      val Array(src, dst) = line.split(",")
      Edge(src.toLong, dst.toLong, 1)
    }

    val graph = Graph.fromEdges(rawEdges, defaultValue = 0)

    println("Running greedy matching...")

    val matchedEdges: RDD[(VertexId, VertexId)] = greedyMatch(graph)

    val matchedCount = matchedEdges.count()

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("==================================")
    println("algorithm completed in " + durationSeconds + "s.")
    println("==================================")
    
    println(s"Matched edges = $matchedCount")
    if (matchedCount == 0) {
      println("WARNING: No edges matched – output will be empty.")
    }

    if (matchedCount > 0) {
      println(s"Saving matched edges to $outputPath ...")
      matchedEdges.map { case (src, dst) => s"$src,$dst" }
        .coalesce(1)
        .saveAsTextFile(outputPath)
    }

    sc.stop()
    println("Program completed successfully.")
  }
}

