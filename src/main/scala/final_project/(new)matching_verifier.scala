package final_project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MatchingVerifier {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: final_project.MatchingVerifier <input_graph.csv> <matching_solution.csv>")
      sys.exit(1)
    }

    val Array(inputGraphPath, matchingPath) = args

    val conf = new SparkConf()
      .setAppName("MatchingVerifier")
      .setMaster("local[*]")
      .set("spark.driver.memory", "4g")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    println(s"Reading input graph from $inputGraphPath...")
    val inputEdges: RDD[(Long, Long)] = sc.textFile(inputGraphPath)
      .map { line =>
        val Array(src, dst) = line.split(",")
        val u = src.toLong
        val v = dst.toLong
        if (u < v) (u, v) else (v, u) // Normalize
      }
      .distinct()

    println(s"Reading matching from $matchingPath...")
    val matchingEdges: RDD[(Long, Long)] = sc.textFile(matchingPath)
      .map { line =>
        val Array(src, dst) = line.split(",")
        val u = src.toLong
        val v = dst.toLong
        if (u < v) (u, v) else (v, u) // Normalize
      }
      .distinct()

    // Step 1: Check if all matching edges exist in the input graph
    val inputEdgeSet = inputEdges.collect().toSet
    val matchingEdgeSet = matchingEdges.collect().toSet

    val invalidEdges = matchingEdgeSet.filterNot(edge => inputEdgeSet.contains(edge))
    if (invalidEdges.nonEmpty) {
      println("❌ Error: The matching contains edges not in the original input graph!")
      invalidEdges.foreach(edge => println(s"Invalid Edge: ${edge._1},${edge._2}"))
      sys.exit(1)
    }

    // Step 2: Check if vertices are reused
    val verticesUsed = matchingEdges.flatMap { case (u, v) => Seq(u, v) }
    val duplicateVertices = verticesUsed.map(v => (v, 1)).reduceByKey(_ + _).filter(_._2 > 1).collect()

    if (duplicateVertices.nonEmpty) {
      println("❌ Error: The matching is invalid! Some vertices are matched more than once:")
      duplicateVertices.foreach { case (vertex, count) =>
        println(s"Vertex $vertex is matched $count times.")
      }
      sys.exit(1)
    }

    // Step 3: If passed, report success
    val totalMatches = matchingEdges.count()
    println(s"✅ Matching is valid.")
    println(s"✅ Total matched pairs: $totalMatches")

    sc.stop()
  }
}

