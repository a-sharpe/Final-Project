package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(graph: Graph[Int, Int]): Graph[Int, Int] = {
    var g = graph.mapVertices((id, _) => 0) // 0 = undecided, 1 = in MIS, -1 = removed
    var keepGoing = true
    var iter = 0
    val startTime = System.currentTimeMillis()

    while (keepGoing) {
      iter += 1
      // Assign random number to all undecided vertices  where (attr == 0)
      val randGraph = g.mapVertices {
        case (id, attr) => if (attr == 0) scala.util.Random.nextDouble() else -1.0
      }

      // Send random values to neighbors
      val neighborMax = randGraph.aggregateMessages[Double](
        sendMsg = triplet => {
          if (triplet.srcAttr != -1.0 && triplet.dstAttr != -1.0) {
            triplet.sendToDst(triplet.srcAttr)
            triplet.sendToSrc(triplet.dstAttr)
          }
        },
        mergeMsg = (a, b) => math.max(a, b)
      )

      // Join and determine if each vertex is a local maxima
      val joined = randGraph.vertices.join(neighborMax)
      val candidateMIS = joined
        .filter { case (vid, (myRand, nbrRand)) => myRand >= nbrRand && myRand != -1.0 }
        .mapValues(_ => 1)

      // Vertices with no neighbors also go into MIS
      val allWithRand = randGraph.vertices
      val candidateWithoutNeighbors = allWithRand
        .subtractByKey(joined)
        .filter { case (vid, rand) => rand != -1.0 }
        .mapValues(_ => 1)

      val newMIS = candidateMIS.union(candidateWithoutNeighbors)

      // Update graph: mark MIS vertices as 1
      val withMIS = g.joinVertices(newMIS) {
        case (vid, oldAttr, newVal) => 1
      }

      // Mark neighbors of new MIS nodes as -1 (removed)
      val neighborsToRemove = withMIS.aggregateMessages[Int](
        sendMsg = triplet => {
          if (triplet.srcAttr == 1 && triplet.dstAttr == 0) triplet.sendToDst(-1)
          if (triplet.dstAttr == 1 && triplet.srcAttr == 0) triplet.sendToSrc(-1)
        },
        mergeMsg = (a, b) => -1
      )

      // Apply neighbor removals
      val updated = withMIS.joinVertices(neighborsToRemove) {
        case (vid, oldAttr, msg) => if (oldAttr == 0) msg else oldAttr
      }

      g = updated

      // Continue if any vertices are still 0 (undecided)
      val remaining = g.vertices.filter { case (id, attr) => attr == 0 }.count()
      println(s"Iteration $iter completed. Remaining undecided vertices: $remaining")
      keepGoing = remaining > 0
    }

    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    val misSize = g.vertices.filter { case (_, attr) => attr == 1 }.count()

    /// Print statments moved here instead of main 
    println("==================================")
    println(f"Luby's algorithm completed in $totalTime%.2f seconds.")
    println(s"Iterations: $iter")
    println(s"MIS size: $misSize")
    println("==================================")

    g
  }

  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    var ans: Boolean = false

    //first check if vertices are adjacent
    //.triplets allows us to combine all the info about vertices and their respective edges
    //then just filter all the triplets that have 1 at the source and destination, these are invalid in MIS

    val invalidCount = g_in.triplets
      .filter(triplet => triplet.srcAttr == 1 && triplet.dstAttr == 1)
      .count()

    if (invalidCount > 0) {
      return ans
    }

    //next check for maximality
    //we can do this by checking that for each 0 vertex, it has at least one 1 vertex neighbor
    //each 1 vertex sends true to neighbors with Agg.Messages
    //we combine messages using OR to find (0,1) vertex pairs

    val hasInMisNeighbor = g_in.aggregateMessages[Boolean](
    sendMsg = ctx => {
      if (ctx.srcAttr == 1) ctx.sendToDst(true)
      if (ctx.dstAttr == 1) ctx.sendToSrc(true)
    },
    mergeMsg = (m1, m2) => m1 || m2,
    tripletFields = TripletFields.All)

    //now compare to original graph: if any 0 vertex has no neighbor in MIS, thenthe graph is not maximal

    val nonMaximalCount = g_in.vertices
      .join(hasInMisNeighbor)            // (vertexId, (attr, neighborHasOne))
      .filter { case (_, (attr, hasNeighborInMIS)) => attr == 0 && !hasNeighborInMIS
      }
      .count()

    if (nonMaximalCount > 0) {
      return ans
    }
    else {
      ans = true
    }

    return ans
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans) {
        println("****** YES IS A MIS *****")
      }
      else {
        println("****** NOT A MIS ******")
      }
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
