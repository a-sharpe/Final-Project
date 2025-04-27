package final_project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

object MaxMatchDriver {

  def lubyMatch(g0: Graph[Int, Unit]): Graph[Int, Unit] = {
    var g = g0.mapVertices((_, _) => 0).cache()
    var cont = true

    while (cont) {
      // 1) assign random weight to free vertices
      val withWeight: Graph[Double, Unit] = g.mapVertices {
        case (_, 0) => scala.util.Random.nextDouble()
        case (_, _) => -1.0
      }

      // 2) compute max neighbor weight
      val maxNbrWeight: VertexRDD[Double] = withWeight.aggregateMessages[Double](
        sendMsg = ctx => {
          if (ctx.srcAttr >= 0) ctx.sendToDst(ctx.srcAttr)
          if (ctx.dstAttr >= 0) ctx.sendToSrc(ctx.dstAttr)
        },
        mergeMsg = math.max
      )

      // 3) attach neighbor-max to each vertex
      val wplus: Graph[(Double, Double), Unit] = withWeight.outerJoinVertices(maxNbrWeight) {
        case (_, myW, Some(m)) => (myW, m)
        case (_, myW, None)    => (myW, Double.NegativeInfinity)
      }

      // 4) mark winning edges
      val winners: Graph[(Double, Double), Int] = wplus.mapTriplets(
        (trip: EdgeTriplet[(Double, Double), Unit]) => {
          val (wSrc, mSrc) = trip.srcAttr
          val (wDst, mDst) = trip.dstAttr
          if (wSrc > mSrc && wDst > mDst) 1 else 0
        },
        TripletFields.All
      )

      // 5) collect winning-edge endpoints → mark them matched
      val newlyMatched: VertexRDD[Int] = winners.aggregateMessages[Int](
        ctx => if (ctx.attr == 1) { ctx.sendToSrc(1); ctx.sendToDst(1) },
        _ | _
      )

      // 6) update vertex states and prune matched vertices
      val updated: Graph[Int, Unit] = g.joinVertices(newlyMatched) {
        case (_, oldState, add) => if (add == 1) 1 else oldState
      }
      g = updated.subgraph(vpred = (_, state) => state == 0).cache()

      cont = g.numEdges > 0
    }

    // bring back final matching state
    g0.outerJoinVertices(
      g0.vertices.context.parallelize(g.vertices.collect())
    ) { case (_, _, s) => s.getOrElse(1) }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("Usage: final_project.MaxMatchDriver compute <graph.csv> <out_path>")
      sys.exit(1)
    }

    val Array(_, inputPath, outputPath) = args
    val conf = new SparkConf()
      .setAppName("MaxMatch")
      .set("spark.driver.memory", "10g")
      .set("spark.executor.memory", "10g")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // Load edges
    val raw = sc.textFile(inputPath).map { line =>
      val a = line.split(",")
      Edge(a(0).toLong, a(1).toLong, ())
    }
    val g0 = Graph.fromEdges(raw, 0)
      .partitionBy(PartitionStrategy.EdgePartition2D)
      .cache()

    // Run matching
    val matched = lubyMatch(g0)

    // Extract matched edges
    val sol = matched.triplets
      .filter(t => t.srcAttr == 1 && t.dstAttr == 1)
      .map(t => s"${t.srcId},${t.dstId}")

    sol.coalesce(1).saveAsTextFile(outputPath)

    sc.stop()
  }
}


