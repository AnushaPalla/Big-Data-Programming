package com.lab

import org.apache.spark._
import org.apache.log4j.{Level, Logger}


object Friends{

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")


    //Controlling log level

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("facebookfriends").setMaster("local[*]");
    val sc = new SparkContext(conf)

    def friendsMapper(line: String) = {
      val words = line.split(" ")
      val key = words(0)
      val pairs = words.slice(1, words.size).map(friend => {
        if (key < friend) (key, friend) else (friend, key)
      })
      pairs.map(pair => (pair, words.slice(1, words.size).toSet))
    }


    def friendsReducer(accumulator: Set[String], set: Set[String]) = {
      accumulator intersect set
    }

    val file = sc.textFile("friend.txt")

    val results = file.flatMap(friendsMapper)
      .reduceByKey(friendsReducer)
      .filter(!_._2.isEmpty)
      .sortByKey()

    results.collect.foreach(line => {
      println(s"${line._1} ${line._2.mkString(" ")}")})

    results.coalesce(1).saveAsTextFile("MutualFacebookFriends")




  }

}