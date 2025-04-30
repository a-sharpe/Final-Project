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
    val degrees: VertexRDD[Int] = graph.degrees.cache()
    val degreeMap = degrees.collectAsMap()
    val degreeBroadcast = sc.broadcast(degreeMap)

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
    var counter = 0

    while (active) {
      val filteredGraph = currentGraph.subgraph(epred = e =>
        !matchedVertices.value.contains(e.srcId) && !matchedVertices.value.contains(e.dstId)
      )

      if (filteredGraph.edges.isEmpty()) {
        active = false
      } else {
        val candidateEdges = filteredGraph.edges
          .map(e => (e.attr, (e.srcId, e.dstId)))
          .sortByKey()
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
      counter += 1
    }
    println(s"Greedy matching completed in $counter iterations.")
    matchedEdges
  }

  def lubyMatch(graphIn: Graph[Int, Int], maxIter: Int = 50)(implicit sc: SparkContext): RDD[(VertexId, VertexId)] = {
    var g: Graph[Int, Double] = graphIn.mapEdges(_ => 0.0)
    var matches = sc.emptyRDD[(VertexId, VertexId)]
    var matchedSet = Set.empty[VertexId]
    var matchedBcast = sc.broadcast(matchedSet)

    var iter = 0

    while (iter < maxIter && g.numEdges > 0) {
      val randG = g.mapEdges(_ => Random.nextDouble())

      val proposals = randG.aggregateMessages[(Double, VertexId)](
        ctx => {
          ctx.sendToSrc((ctx.attr, ctx.dstId))
          ctx.sendToDst((ctx.attr, ctx.srcId))
        },
        (a, b) => if (a._1 > b._1) a else b
      )

      val candidatePairs = proposals.flatMap { case (vid, (prio, nbr)) =>
        Iterator(((math.min(vid, nbr), math.max(vid, nbr)), vid))
      }

      val selected: RDD[(VertexId, VertexId)] = candidatePairs
        .groupByKey(2)
        .filter(_._2.size == 2)
        .keys

      val newly = selected.flatMap { case (u, v) => Seq(u, v) }.collect().toSet

      if (newly.nonEmpty) {
        matches = matches.union(selected)
        matchedSet ++= newly
        matchedBcast.unpersist(blocking = false)
        matchedBcast = sc.broadcast(matchedSet)
      }

      g = randG.subgraph(vpred = (vid, _) => !matchedBcast.value.contains(vid))
      iter += 1
    }
    println(s"Luby's matching completed in $iter iterations.")
    matches.distinct()
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println("Usage: final_project.MaximalMatchingDriver compute <input_graph.csv> <output_dir> <algorithm: greedy|luby>")
      sys.exit(1)
    }

    val Array(_, inputPath, outputPath, algorithm) = args

    val conf = new SparkConf()
      .setAppName("MaximalMatchingDriver")
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
    }
    val scLocal = new SparkContext(conf)
    implicit val sc: SparkContext = scLocal
    val spark = SparkSession.builder.config(conf).getOrCreate()

    println(s"Reading input graph from $inputPath...")

    val rawEdges: RDD[Edge[Int]] = sc.textFile(inputPath)
      .filter(_.nonEmpty)
      .map { line =>
        val Array(src, dst) = line.split(",", -1)
        Edge(src.toLong, dst.toLong, 1)
      }

    val graph = Graph.fromEdges(rawEdges, defaultValue = 0)
      .partitionBy(EdgePartition2D, 96)

    println(s"Running $algorithm maximal matching...")

    val matched: RDD[(VertexId, VertexId)] = algorithm.toLowerCase match {
      case "greedy" => greedyMatchDistributed(graph)
      case "luby"   => lubyMatch(graph)
      case other =>
        System.err.println(s"Unknown algorithm: '$other'. Use 'greedy' or 'luby'.")
        sys.exit(1)
        sc.emptyRDD
    }

    val matchedCount = matched.count()
    println(s"Matched edges = $matchedCount")

    if (matchedCount > 0) {
      println(s"Saving matched edges to $outputPath...")
      matched.map { case (src, dst) => s"$src,$dst" }
        .coalesce(1)
        .saveAsTextFile(outputPath)
    } else {
      println("WARNING: No edges matched – output will be empty.")
    }

    sc.stop()
    println("Program completed successfully.")
  }
}

