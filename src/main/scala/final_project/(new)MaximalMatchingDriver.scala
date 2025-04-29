package final_project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MaximalMatchingDriver {

  var sc: SparkContext = _

  def maximalMatching(graph: Graph[Int, Int]): RDD[(VertexId, VertexId)] = {
    // Precompute degrees
    val degrees: VertexRDD[Int] = graph.degrees.cache()
    val degreeMap = degrees.collectAsMap()
    val degreeBroadcast = sc.broadcast(degreeMap)

    // Enrich edges with priority: min(degree(src), degree(dst))
    val prioritizedGraph = graph.mapTriplets(triplet => {
      val degreeMapLocal = degreeBroadcast.value
      val srcDegree = degreeMapLocal.getOrElse(triplet.srcId, Int.MaxValue)
      val dstDegree = degreeMapLocal.getOrElse(triplet.dstId, Int.MaxValue)
      math.min(srcDegree, dstDegree)
    })

    var currentGraph = prioritizedGraph
    var matchedVertices = sc.broadcast(Set.empty[VertexId])
    var matchedEdges = sc.emptyRDD[(VertexId, VertexId)]

    var active = true

    while (active) {
      val filteredGraph = currentGraph.subgraph(epred = e =>
        !matchedVertices.value.contains(e.srcId) && !matchedVertices.value.contains(e.dstId)
      )

      if (filteredGraph.edges.isEmpty()) {
        active = false
      } else {
        val candidateEdges = filteredGraph.edges
          .map(e => (e.attr, (e.srcId, e.dstId))) // (priority, (u,v))
          .sortByKey() // lower degree first
          .values
          .collect()

        val newMatches = scala.collection.mutable.ListBuffer.empty[(VertexId, VertexId)]
        val alreadyMatched = scala.collection.mutable.Set.empty[VertexId]

        for ((u, v) <- candidateEdges) {
          if (!alreadyMatched.contains(u) && !alreadyMatched.contains(v)) {
            newMatches += ((u, v))
            alreadyMatched += u
            alreadyMatched += v
          }
        }

        val newMatchesRDD = sc.parallelize(newMatches)

        if (newMatchesRDD.isEmpty()) {
          active = false
        } else {
          matchedVertices = sc.broadcast(matchedVertices.value ++ alreadyMatched)
          matchedEdges = matchedEdges.union(newMatchesRDD)
        }
      }
    }

    matchedEdges
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("Usage: final_project.MaximalMatchingDriver compute <input_graph.csv> <output_dir>")
      sys.exit(1)
    }

    val Array(_, inputPath, outputPath) = args

    val conf = new SparkConf()
      .setAppName("MaximalMatchingDriver")
      .setMaster("local[*]")
      .set("spark.driver.memory", "8g")
    sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    println(s"Reading input graph from $inputPath...")

    val rawEdges: RDD[Edge[Int]] = sc.textFile(inputPath).map { line =>
      val Array(src, dst) = line.split(",")
      Edge(src.toLong, dst.toLong, 1)
    }

    val graph = Graph.fromEdges(rawEdges, defaultValue = 0)

    println("Running maximal matching...")

    val matchedEdges: RDD[(VertexId, VertexId)] = maximalMatching(graph)

    val matchedCount = matchedEdges.count()
    println(s"Matched edges = $matchedCount")

    if (matchedCount > 0) {
      println(s"Saving matched edges to $outputPath...")
      matchedEdges.map { case (src, dst) => s"$src,$dst" }
        .coalesce(1)
        .saveAsTextFile(outputPath)
    } else {
      println("WARNING: No edges matched – output will be empty.")
    }

    sc.stop()
    println("Program completed successfully.")
  }
}

