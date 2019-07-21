import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._


object Firstprogramicp6 {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val conf = new SparkConf().setMaster("local[2]").setAppName("GraphFrames")
    val scontext = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("GraphFrames")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val trips = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Anusha Reddy\\Downloads\\Datasets\\201508_trip_data.csv")

    val stations = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Anusha Reddy\\Downloads\\Datasets\\201508_station_data.csv")



    // Printing the Schema

    trips.printSchema()

    stations.printSchema()


    trips.createOrReplaceTempView("Trips")

    stations.createOrReplaceTempView("Stations")


    val station = spark.sql("select * from Stations")

    val trip = spark.sql("select * from Trips")

    val stationVertices = station
      .withColumnRenamed("name", "id")
      .distinct()

    val tripEdges = trip
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")

    val stationsGraph = GraphFrame(stationVertices, tripEdges)

    tripEdges.cache()
    stationVertices.cache()
    stationsGraph.vertices.show()
    stationsGraph.edges.show()

    // Triangle Count
    val stationTriCount = stationsGraph.triangleCount.run()
    stationTriCount.select("id","count").show()
    println("Traingle Count ");

    // Shortest Path
    val shortestPath = stationsGraph.shortestPaths.landmarks(Seq("Golden Gate at Polk","MLK Library")).run
    shortestPath.show()

    println("Shortest Path");


    //Page Rank
    val stationsPageRank = stationsGraph.pageRank.resetProbability(0.15).tol(0.01).run()
    stationsPageRank.vertices.select("id", "pagerank").show()
    stationsPageRank.edges.show()

    println("Label Progragation ");

    val lpa = stationsGraph.labelPropagation.maxIter(5).run()
    lpa.show()
    lpa.select("id", "label").show()


    // BFS
    val pathBFS = stationsGraph.bfs.fromExpr("id = 'Japantown'").toExpr("dockcount < 10").run()
    pathBFS.show()

    println("BFS ");


    //Saving to File
    stationsGraph.vertices.write.csv("C:\\Users\\Anusha Reddy\\IdeaProjects\\Sparkicp5\\op3")

    stationsGraph.edges.write.csv("C:\\Users\\Anusha Reddy\\IdeaProjects\\Sparkicp5\\op4")

  }

}