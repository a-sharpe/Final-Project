package final_project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.util.Random

object MaximalMatchingDriver {

  def greedyMatchDistributed(graph: Graph[Int, Int])(implicit sc: SparkContext): RDD[(VertexId, VertexId)] = {
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

  def lubyMatch(graphIn: Graph[Int, Int], maxIter: Int = 50)(implicit sc: SparkContext): RDD[(VertexId, VertexId)] = {

    var g: Graph[Int, Double] = graphIn.mapEdges(_ => 0.0)
    var matches = sc.emptyRDD[(VertexId,VertexId)]
    var matchedSet = Set.empty[VertexId]
    var matchedBcast = sc.broadcast(matchedSet)

    var iter = 0

    while (iter < maxIter && g.numEdges > 0) {

      // Step 1: assign fresh random priorities to each edge (0.0‑1.0)
      val randG: Graph[Int, Double] = g.mapEdges(_ => Random.nextDouble())

      // Step 2: each vertex picks the incident edge with highest priority
      val proposals: VertexRDD[(Double, VertexId)] = randG.aggregateMessages[(Double, VertexId)](
        ctx => {
          ctx.sendToSrc((ctx.attr, ctx.dstId))
          ctx.sendToDst((ctx.attr, ctx.srcId))
        },
        (a, b) => if (a._1 > b._1) a else b
      )

      // Step 3: build candidate pairs and keep those endorsed by *both* endpoints
      val candidatePairs = proposals.flatMap { case (vid, (prio, nbr)) =>
        Iterator(((math.min(vid, nbr), math.max(vid, nbr)), vid))
      }

      val selected: RDD[(VertexId, VertexId)] = candidatePairs
        .groupByKey(2)               // two keys expected (each endpoint)
        .filter(_._2.size == 2)      // both ends agree
        .keys

      val newly = selected.flatMap { case (u,v) => Seq(u,v) }.collect().toSet

      if (newly.nonEmpty) {
        matches = matches.union(selected)
        matchedSet ++= newly
        matchedBcast.unpersist(blocking = false)
        matchedBcast = sc.broadcast(matchedSet)
      }

    // filter out matched vertices via broadcast
      g = randG.subgraph(vpred = (vid, _) => !matchedBcast.value.contains(vid))
      iter += 1
    }
    matches.distinct()
  }

  // ---------------------------------------------------------------------------
  // Driver entry point
  // ---------------------------------------------------------------------------
  def main(args: Array[String]): Unit = {

    if (args.length < 3 || args.length > 4) {
      System.err.println("Usage: final_project.main3 compute <input_graph.csv> <output_dir> <algorithm>")
      sys.exit(1)
    }

    val inputPath  = args(1)
    val outputPath = args(2)
    val algorithm  = if (args.length == 4) args(3).toLowerCase else "greedy"
    val conf = new SparkConf().setAppName("DistributedGreedyMaxMatching")
    // If launched without a cluster manager, fall back to local mode
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    implicit val sc    = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    val startTimeMillis = System.currentTimeMillis()

    // Edge list assumed as `src,dst` per line
    val rawEdges: RDD[Edge[Int]] = sc.textFile(inputPath, 200)
      .filter(_.nonEmpty)
      .map { line =>
        val Array(src, dst) = line.split(",", -1)
        Edge(src.toInt, dst.toInt, 1.toByte)
      }

    val graph = Graph.fromEdges(rawEdges, defaultValue = 0).partitionBy(EdgePartition2D, 96)

    val matched: RDD[(VertexId, VertexId)] = algorithm match {
      case "luby"   => lubyMatch(graph)
      case "greedy" => greedyMatchDistributed(graph)
      case other     =>
        System.err.println(s"Unknown algorithm '$other'. Use 'greedy' or 'luby'.")
        sys.exit(1)
        null // unreachable
    }

    val matchedCount = matched.count()

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("==================================")
    println(algorithm + " completed in " + durationSeconds + "s.")
    println(s"Matched edges = $matchedCount")
    println("==================================")

    // Write each partition separately; combine later with `cat` or `hadoop fs -getmerge` if needed
    matched.map { case (u, v) => s"$u,$v" }.coalesce(1).saveAsTextFile(outputPath)

    sc.stop()

    println("==================================")
    println( "ALGORITHM COMPLETED")
    println("==================================")
  }
}
