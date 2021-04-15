package day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01")
    val ssc = new StreamingContext(conf,Seconds(10))
    val lines=ssc.socketTextStream("192.168.137.61",9999)
    val works =lines.flatMap(_.split(" "))
    val pairs = works.map(works=>(works,1))
    val wordCounts= pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
