package final_project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

object MaxMatchDriver {

  def lubyMatch(g0: Graph[Int,Unit], seed: Long): Graph[Int,Unit] = {
    // —— copy your loop structure from LubyMIS, but:
    //    • vertex attr: 0 = free, 1 = matched
    //    • use two-phase msg passing to elect “winning edges”
    //    • joinVertices to mark attr=1 on both endpoints
    //    • prune subgraph(vpred = (id,attr)=>attr==0)
    var g = g0.mapVertices((_,_) => 0)
    var cont = true
    while (cont) {
      // 1) assign rand weight to free vertices
      val withWeight = g.mapVertices {
        case (vid, 0) => scala.util.Random.nextDouble()
        case (vid, st) => -1.0
      }
      // 2) for each vertex, send your weight to neighbors; each edge
      //    learns max competitor weight at src & dst
      val maxNbrWeight = withWeight.aggregateMessages[(Double,Double)](
        sendMsg = triplet => {
          if (triplet.srcAttr >= 0) {
            triplet.sendToDst((triplet.srcAttr, Double.NegativeInfinity))
          }
          if (triplet.dstAttr >= 0) {
            triplet.sendToSrc((Double.NegativeInfinity, triplet.dstAttr))
          }
        },
        mergeMsg = { case ((a1,b1),(a2,b2)) => (math.max(a1,a2), math.max(b1,b2)) }
      )
      // 3) mark “winning” edges
      val winners = withWeight.mapTriplets { trip =>
        val (maxAtSrc, maxAtDst) = maxNbrWeight.lookup(trip.srcId).headOption.getOrElse((Double.PositiveInfinity,Double.PositiveInfinity))
        val win = trip.srcAttr > maxAtSrc && trip.dstAttr > maxAtDst
        if (win) 1 else 0
      }
      // 4) collect winners → mark endpoints matched
      val matchedVerts = winners.aggregateMessages[Int](
        ctx => {
          if (ctx.attr == 1) {
            ctx.sendToSrc(1); ctx.sendToDst(1)
          }
        },
        _ | _
      )
      g = g.joinVertices(matchedVerts)((vid, old, add) => if (add == 1) 1 else old)
      // 5) prune
      g = g.subgraph(vpred = (id, st) => st == 0)
      cont = g.numEdges > 0
    }
    g
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("Usage: final_project.MaxMatchDriver compute <graph.csv> <out_path>")
      sys.exit(1)
    }

    val conf = new SparkConf().setAppName("MaxMatch")
    val sc   = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    val inputPath  = args(1)
    val outputPath = args(2)

    // load edges exactly as in your verifier, but attr = ()
    val raw = sc.textFile(inputPath).map { line =>
      val a = line.split(",")
      Edge(a(0).toLong, a(1).toLong, ())
    }
    val g0 = Graph.fromEdges(raw, 0)
      .partitionBy(PartitionStrategy.EdgePartition2D)
      .cache()

    // run Luby‐style matching
    val matchedGraph = lubyMatch(g0, seed = System.currentTimeMillis)

    // extract the matched edges
    val sol = matchedGraph.triplets
      .filter(t => t.srcAttr == 1 && t.dstAttr == 1)
      .map(t => s"${t.srcId},${t.dstId}")

    // save exactly one part so you can hand it to the verifier
    sol.coalesce(1).saveAsTextFile(outputPath)
  }
}
