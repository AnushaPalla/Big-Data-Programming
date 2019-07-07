import org.apache.spark.{SparkConf, SparkContext}
object BFS {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    val conf = new SparkConf().setAppName("Breadthfirstsearch").setMaster("local[*]")
    val scontext = new SparkContext(conf)
    class Graph[T] {
      type Vertex = T
      type GraphMap = Map[Vertex, List[Vertex]]
      var g: GraphMap = Map()

      def BFS(start: Vertex): List[List[Vertex]] = {

        def BFS0(elems: List[Vertex], visited: List[List[Vertex]]): List[List[Vertex]] = {
          val newNeighbors = elems.flatMap(g(_)).filterNot(visited.flatten.contains).distinct
          if (newNeighbors.isEmpty)
            visited
          else
            BFS0(newNeighbors, newNeighbors :: visited)
        }

        BFS0(List(start), List(List(start))).reverse
      }

    }
    var graph1 = new Graph[Int]
    graph1 .g = Map(1 -> List(2,4), 2-> List(1,3), 3-> List(2,4), 4-> List(1,3))
    println(graph1 .BFS(1))

  }
}