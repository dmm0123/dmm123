package day01
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
object WorldCount1 {
  def main(args: Array[String]): Unit = {
    // 定义更新状态方法，参数values为当前批次单词频度，state为以往批次单词频度
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
      val conf = new SparkConf().setMaster("local[2]").setAppName("WorldCount1")
      val ssc = new StreamingContext(conf, Seconds(3))
      ssc.checkpoint(".")
      // Create a DStream that will connect to hostname:port, like localhost:9999
      val lines = ssc.socketTextStream("master", 9999)
      // Split each line into words
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b),Seconds(12), Seconds(6))

      wordCounts.print()

      ssc.start()             // Start the computation
      ssc.awaitTermination()  // Wait for the computation to terminate
      //ssc.stop()

  }
}
