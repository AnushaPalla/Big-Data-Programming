package com.lab

import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j._

object Fifa {

  def main(args: Array[String]): Unit = {

    //Setting up the Spark Session and Spark Context
    val conf = new SparkConf().setMaster("local[2]").setAppName("Fifa")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("FIFA World Cup Analysis")
      .config(conf = conf)
      .getOrCreate()


    // Creating WorldCups Dataframe using StructType
    val customSchema = StructType(Array(
      StructField("Year", IntegerType, true),
      StructField("Country", StringType, true),
      StructField("WinnerTeam", StringType, true),
      StructField("RunnerupTeam", StringType, true),
      StructField("ThirdTeam", StringType, true),
      StructField("FourthTeam", StringType, true),
      StructField("TotalNoOfGoals", IntegerType, true),
      StructField("TotalNoOfQualifiedTeams", IntegerType, true),
      StructField("TotalNoOfMatchesPlayed", IntegerType, true),
      StructField("Attendance", DoubleType, true)))

    val worldcup_df = spark.sqlContext.read.format("csv")
      .option("delimiter",",")
      .option("header", "true")
      .schema(customSchema)
      .load("WorldCups.csv")
    // No of goals greater than 3
    worldcup_df.createGlobalTempView("worldcup")

    spark.sql("SELECT Year, Attendance ,Country, TotalNoOfGoals FROM global_temp.worldcup where TotalNoOfGoals>=3").show()
    //  Top teams that won highest number of WorldCups

    worldcup_df.groupBy("WinnerTeam").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    //  Countries that hosted most number of WorldCups

    worldcup_df.groupBy("Country").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    // Countries that hosted and won the WorldCup

    worldcup_df.createGlobalTempView("worldcup1")

    spark.sql("SELECT Country,WinnerTeam FROM global_temp.worldcup1 where Country==WinnerTeam").show()

    // Total number of Goals Scored from the years 1930 to 2014

    worldcup_df.agg(sum("TotalNoOfGoals")).show()

    //  Top teams that missed WorldCup and placed in Runner-Up

    worldcup_df.groupBy("RunnerupTeam").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    // Percentage of Goals required to Qualify for the match
    import spark.sqlContext.implicits._

    worldcup_df.withColumn("Percentage",$"TotalNoOfQualifiedTeams"/$"TotalNoOfGoals"*100).show()

    // Highest attendance for the matches

    worldcup_df.agg(max("Attendance")).show

    //  Top teams that missed WorldCup and placed in ThirdTeam

    worldcup_df.groupBy("ThirdTeam").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    //  Top teams that missed WorldCup and placed in FourthTeam

    worldcup_df.groupBy("FourthTeam").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    //  Total number of Goals scored by each country in all the years
    worldcup_df.createGlobalTempView("worldcup2")


    spark.sql("SELECT Country,sum(TotalNoOfGoals) FROM global_temp.worldcup2 group by Country").show()

    
  }

}