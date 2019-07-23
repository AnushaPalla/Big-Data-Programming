package com.lab

import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j._


object FifaRDDS{

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils");
    //Creating Spark Session and Spark Context
    val conf = new SparkConf().setMaster("local[2]").setAppName("Fifa")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("FifaRdds")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)


    //Import the dataset and create df and print Schema

    val wc_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Anusha Reddy\\Downloads\\WorldCups.csv")

    val wcplayers_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Anusha Reddy\\Downloads\\WorldCupPlayers.csv")


    val wcmatches_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Anusha Reddy\\Downloads\\WorldCupMatches.csv")


    // Printing the Schema

    wc_df.printSchema()

    wcmatches_df.printSchema()

    wcplayers_df.printSchema()

    //create three Temp View

    wc_df.createOrReplaceTempView("WorldCup")

    wcmatches_df.createOrReplaceTempView("wcMatches")

    wcplayers_df.createOrReplaceTempView("wcPlayers")

    //Perform any 5 queries in Spark RDD’s and Spark Data Frames.

    // RDD creation

    val csv = sc.textFile("C:\\Users\\Anusha Reddy\\Downloads\\WorldCups.csv")

    val header = csv.first()

    val data = csv.filter(line => line != header)

    val rdd = data.map(line=>line.split(",")).collect()

    // Highest Number of Goals scored

    //RDD
    println("query1");

    val rddgoals = data.filter(line => line.split(",")(6) != "NULL").map(line => (line.split(",")(1),
      (line.split(",")(6)))).takeOrdered(10)
    rddgoals.foreach(println)

    // Dataframe
    wc_df.select("Country","GoalsScored").orderBy("GoalsScored").show(10)



    // Winning country ordered by years

    //RDD
    println("query2");

    val rddvenue = data.filter(line => line.split(",")(1)==line.split(",")(2))
      .map(line => (line.split(",")(0),line.split(",")(1), line.split(",")(2)))
      .collect()

    rddvenue.foreach(println)

    //Dataframe
    wc_df.select("Year","Country","Winner").filter("Country==Winner").show(10)



    //Details of years

    // RDD
    println("query3");

    var years = Array("1930","1950","1970","1990","2010")

    val rddwinY = data.filter(line => (line.split(",")(0)=="1950" ))
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddwinY.foreach(println)

    //DataFrame
    wc_df.select("Year","Runners-Up","Winner").filter("Year='1950' or Year='1930' or " +
      "Year='1990' or Year='1970' or Year='2010'").show(10)



    // Details of the World Cup match organised in 2011.

    //RDD
    println("query4");

    val rddStat = data.filter(line=>line.split(",")(0)=="2011")
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddStat.foreach(println)

    //Dataframe
    wc_df.filter("Year=2011").show()


    //Maximum NUmber of Matches Played

    //RDD
    println("query5");

    val rddMax = data.filter(line=>line.split(",")(8) == "64")
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddMax.foreach(println)

    // DataFrame
    wc_df.filter("MatchesPlayed == 64").show()



  }
}