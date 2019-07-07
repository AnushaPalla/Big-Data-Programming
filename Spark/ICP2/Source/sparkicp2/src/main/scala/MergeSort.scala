//importing required packages
import org.apache.log4j.{Level, Logger}
import org.apache.spark._


object MergeSort {

  def main(args: Array[String]): Unit = {


    //Controlling log level

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir","C:\\winutils" )

    //running Spark in local mode, so we  need to set master to local

    val conf =   new SparkConf().setAppName("mergeSort").setMaster("local");
    val sc   =   new SparkContext(conf);
  //array that needs to be sorted
    val s = Array(101,30,34,23,19,75,24,7,4);

    val sarray = sc.parallelize(Array(101,30,34,23,19,75,24,7,4));


    val maparray = sarray.map(x=>(x,1))

    val sorted = maparray.sortByKey();



    sorted.keys.collect().foreach(println)

    //length of array
    val n =  s.length;

    val l = 0;

    val r = n-1;

    print("Input array\n")
    for ( x <-  s)
    {
      print(x+",");
    }

    sort(s,l,r);

    print("\nArray after merge sort\n")
    for ( y <- s)
    {
      print(y+",")
    }



    // Sorting Array
    def sort(arr: Array[Int], l: Int, r: Int): Unit = {
      if (l < r) {
        val m = (l + r) / 2
        // Sort first and second part
        sort(arr, l, m)
        sort(arr, m + 1, r)
        // Merge the sorted part
        merge(arr, l, m, r)
      }
    }


    // Merge for Array
    def merge(arr: Array[Int], l: Int, m: Int, r: Int): Unit = {

      // sizes of two subarrays that needs to be merged
      val n1 = m - l + 1
      val n2 = r - m


     //create temporary arrays
      val Left = new Array[Int](n1)
      val Right = new Array[Int](n2)

     //copy data to those temporary arrays

      var a = 0
      while (a < n1){
        Left(a) = arr(l + a);
        a += 1;
      }
      var b = 0
      while (b < n2){
        Right(b) = arr(m + 1 + b);
        b += 1;
      }



      var i = 0
      var j = 0
      // Initial index of merged subarray
      var k = l
      while (i < n1 && j < n2) {
        if (Left(i) <= Right(j)) {
          arr(k) = Left(i)
          i += 1
        }
        else {
          arr(k) = Right(j)
          j += 1
        }
        k += 1
      }


      while (i < n1)
      {
        arr(k) = Left(i)
        i += 1
        k += 1
      }

      while (j < n2)
      {
        arr(k) = Right(j)
        j += 1
        k += 1
      }

    }

  }



}