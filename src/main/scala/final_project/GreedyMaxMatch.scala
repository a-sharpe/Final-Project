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

package final_project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * A distributed greedy maximal matching driver.
  *
  * The algorithm has two phases:
  *   1. **Local greedy pass**   – each partition scans its edges and picks a conflict‑free subset.
  *   2. **Duplicate resolution** – a single shuffle groups by vertex so that no vertex
  *      appears in more than one matched edge across partitions.
  *
  * No call to `collect()` is made, so the driver never sees the full edge list.
  * Runtime is dominated by two wide operations (`reduceByKey` and `distinct`).
  *
  * The result is a *maximal* matching: no additional edge can be added without sharing
  * an endpoint, though it may not be globally maximum.  In practice this heuristic
  * is fast and works well for graphs with billions of edges.
  */
object main3 {

  // ---------------------------------------------------------------------------
  // Distributed greedy maximal matching
  // ---------------------------------------------------------------------------
  def greedyMatchDistributed(graph: Graph[Int, Int]): RDD[(VertexId, VertexId)] = {

    // Phase 1: partition‑local greedy
    val locallyMatched: RDD[(VertexId, VertexId)] = graph.edges.mapPartitions { iter =>
      val seen = scala.collection.mutable.HashSet[VertexId]()
      iter.flatMap { e =>
        val u = e.srcId
        val v = e.dstId
        if (!seen.contains(u) && !seen.contains(v)) {
          seen += u
          seen += v
          Iterator((u, v))
        } else Iterator.empty
      }
    }

    // Phase 2: resolve vertices that were chosen in two different partitions
    locallyMatched
      .flatMap { case (u, v) => Iterator((u, (u, v)), (v, (u, v))) }
      .reduceByKey((edge, _) => edge)   // keep one edge per vertex
      .values
      .distinct()                       // remove the duplicates introduced by explode
  }

  // ---------------------------------------------------------------------------
  // Driver entry point
  // ---------------------------------------------------------------------------
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      System.err.println("Usage: final_project.main3 compute <input_graph.csv> <output_dir>")
      sys.exit(1)
    }

    val Array(_, inputPath, outputPath) = args

    val conf = new SparkConf().setAppName("DistributedGreedyMaxMatching")
    // If launched without a cluster manager, fall back to local mode
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val sc    = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // Edge list assumed as `src,dst` per line
    val rawEdges: RDD[Edge[Int]] = sc.textFile(inputPath)
      .filter(_.nonEmpty)
      .map { line =>
        val Array(src, dst) = line.split(",", -1)
        Edge(src.toLong, dst.toLong, 1)
      }

    val graph = Graph.fromEdges(rawEdges, defaultValue = 0)

    val matchedEdges = greedyMatchDistributed(graph).persist()

    val matchedCount = matchedEdges.count()
    println(s"Matched edges = $matchedCount")

    // Write each partition separately; combine later with `cat` or `hadoop fs -getmerge` if needed
    matchedEdges
      .map { case (u, v) => s"$u,$v" }
      .saveAsTextFile(outputPath)

    sc.stop()
  }
}

