package com.lab

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object Graphxtask {

  def main(args: Array[String]): Unit = {
  // spark configuration setting master to local
    val conf = new SparkConf().setMaster("local[2]").setAppName("PAGE RANK")
    val sc = new SparkContext(conf)
    //create or get sparksession
    val spark = SparkSession
      .builder()
      .appName("PAGE RANK")
      .config(conf = conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    // creating dataframes
    val edges_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Anusha Reddy\\Downloads\\group-edges.csv")

    val groups_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Anusha Reddy\\Downloads\\meta-groups.csv")



    // Printing the Schema

    edges_df.printSchema()

    groups_df.printSchema()


    edges_df.createOrReplaceTempView("e")

    groups_df.createOrReplaceTempView("g")


    val g1 = spark.sql("select * from g")

    val e1 = spark.sql("select * from e")
    //replacing column names
    val vertices = g1
      .withColumnRenamed("group_id", "id").limit(100)
      .distinct()

    val edges = e1
      .withColumnRenamed("group1", "src").limit(500).distinct()
      .withColumnRenamed("group2", "dst").limit(500).distinct()


    val graph = GraphFrame(vertices, edges)

    edges.cache()
    vertices.cache()
    graph.vertices.show()
    graph.edges.show()


    println("Total Number of vertices count is : " + graph.vertices.count)
    println("Total Number of edges count is: " + graph.edges.count)


    val stationPageRank = graph.pageRank.resetProbability(0.15).tol(0.01).run()
    stationPageRank.vertices.show()
    stationPageRank.edges.show()


  }

}