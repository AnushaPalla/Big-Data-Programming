
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");
      //running Spark in local mode, so we  need to set master to local
    // Spark configuration

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    // create Spark context with Spark configuration

    val scontext=new SparkContext(sparkConf)

    val input =  scontext.textFile("input.txt")

    val output = "output"
    // read in text file and split each document into words

    val words = input.flatMap(line => line.split("\\W+"))

    words.foreach(f=>println(f))
    // Transform into word and count the words
    val count = words.map(words => (words, 1)).reduceByKey(_+_,1)

    val wordsList=count.sortBy(outputList=>outputList._1,ascending = true)

    wordsList.foreach(outputList=>println(outputList))
   //output file
    wordsList.saveAsTextFile(output)

    wordsList.take(10).foreach(outputList=>println(outputList))

    scontext.stop()

  }

}