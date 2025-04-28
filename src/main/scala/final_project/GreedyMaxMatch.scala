package final_project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object main3 {

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
  
  // Driver entry point
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      System.err.println("Usage: final_project.main3 compute <input_graph.csv> <output_dir>")
      sys.exit(1)
    }

    val Array(_, inputPath, outputPath) = args

    val conf = new SparkConf().setAppName("DistributedGreedyMaxMatching")
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val sc    = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    val startTimeMillis = System.currentTimeMillis()
    
    val rawEdges: RDD[Edge[Int]] = sc.textFile(inputPath)
      .filter(_.nonEmpty)
      .map { line =>
        val Array(src, dst) = line.split(",", -1)
        Edge(src.toLong, dst.toLong, 1)
      }

    val graph = Graph.fromEdges(rawEdges, defaultValue = 0)

    val matchedEdges = greedyMatchDistributed(graph).persist()

    val matchedCount = matchedEdges.count()

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("==================================")
    println("algorithm completed in " + durationSeconds + "s.")
    println(s"Matched edges = $matchedCount")
    println("==================================")
    
    matchedEdges
      .map { case (u, v) => s"$u,$v" }
      .saveAsTextFile(outputPath)

    sc.stop()
    println("Program completed successfully.")
  }
}
