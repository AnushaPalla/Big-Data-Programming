import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession

object ICP3part1  {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","C:\\winutils" );

    val conf = new SparkConf().setMaster("local[2]").setAppName("my app")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._


    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Kavya Reddy\\IdeaProjects\\sparkicp3\\survey.csv")


    //df.show()

    df.write.format("csv").option("header","true").save("C:\\Users\\Kavya Reddy\\IdeaProjects\\sparkicp3\\output1")
    //df.write.csv("C:\\Users\\Lenovo\\IdeaProjects\\M2_ICP3\\survey_savefile.csv")

    //Apply Union operation on the dataset and order the output by Country Name alphabetically.
    val df1 = df.limit(5)
    val df2 = df.limit(10)
    df1.show()
    df2.show()
    val unionDf = df1.union(df2)
    println("Union")
    unionDf.orderBy("Country").show()


    df.createOrReplaceTempView("survey")

    // Duplicate

    val DupDF = spark.sql("select COUNT(*),Country,Timestamp from survey GROUP By Timestamp,Country Having COUNT(*) > 1")
    //println("Check duplicate")
    //val uniq=df.dropDuplicates().show()
    val uniq= df.groupBy("Timestamp","Age","Gender","Country","state","self_employed","family_history","treatment","work_interfere","no_employees","remote_work","tech_company","benefits","care_options").count.filter($"count">1).show()
    val total=df.count()
    //DupDF.show(50)
    println("no of duplicate records:"+uniq)
    println("no of total records:"+total)



    //Use Groupby Query based on treatment.

    val tregroup = spark.sql("select count(Country) from survey GROUP BY treatment ")
    println("group by treatment")
    tregroup.show()


    //Aggregate Max and Average
    val MaxDF = spark.sql("select Max(Age) from survey")
    println("Maximum of age")
    MaxDF.show()

    val AvgDF = spark.sql("select Avg(Age) from survey")
    println("Average of age")
    AvgDF.show()



    // Join the dataframe using sql

    val df3 = df.limit(50)
    val df4 = df.limit(80)

    df3.createOrReplaceTempView("left")
    df4.createOrReplaceTempView("right")


    val joinSQl = spark.sql("select left.state,right.Country FROM left,right where left.Age = " +
      "right.Age")
    println("left join and right join")
    joinSQl.show()

    //13th Row from DataFrame
    val df13th = df.take(13).last
    println("13th row of dataset")
    print(df13th)


    def parseLine(line: String) =
    {
      val fields = line.split(",")
      val timestamp = fields(0).toString
      val age = fields(1).toString
      val noemply = fields(9).toString
      (timestamp,age, noemply)
    }

    val lines = sc.textFile("C:\\Users\\Kavya Reddy\\IdeaProjects\\sparkicp3\\survey.csv")
    val rdd = lines.map(parseLine).toDF()
    println("")
    println("After parse Line")
    rdd.show()
  }
}