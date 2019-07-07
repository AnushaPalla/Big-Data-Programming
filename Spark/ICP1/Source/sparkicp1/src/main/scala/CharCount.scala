
import org.apache.spark.{SparkContext, SparkConf}

object CharCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");

    val sparkConf = new SparkConf().setAppName("CharCount").setMaster("local[*]")

    val scontext=new SparkContext(sparkConf)
  //input file
    val input =  scontext.textFile("charcount.txt")
    //output file
    val output = "output2"

    val words = input.flatMap(line => line.split(""))

    words.foreach(f=>println(f))
  //mapping and reducing
    val count = words.map(words => (words, 1)).reduceByKey(_+_,1)

    val wordsList=count.sortBy(outputList=>outputList._1,ascending = true)

    wordsList.foreach(outputList=>println(outputList))
    wordsList.saveAsTextFile(output)

    wordsList.take(10).foreach(outputList=>println(outputList))
    scontext.stop()

  }

}