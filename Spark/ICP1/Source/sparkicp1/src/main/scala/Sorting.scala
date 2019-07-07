
import org.apache.spark.{ SparkConf, SparkContext }

object Sorting{
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\winutils" )
    //running Spark in local mode, so we  need to set master to local
    val conf = new SparkConf().setAppName("Sorting").setMaster("local[*]")
    //create sparkcontext with configuration
    val scontext = new SparkContext(conf)

    val rdd = scontext.textFile("sorting.txt")
    val result = rdd.map(_.split(",")).map { m => ((m(0), m(1)),m(2))}

    result .foreach { println }
    val numberOfReducers = 4;

    val listrdd = result.groupByKey(numberOfReducers).mapValues(iteration => iteration.toList.sortBy(k => k))

  //output file

    listrdd.saveAsTextFile("output1");

  }
}