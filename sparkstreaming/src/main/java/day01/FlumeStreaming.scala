package day01
import java.net.InetSocketAddress

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
object FlumeStreaming {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("FlumeStreaming").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(15))
    val address = Seq(new InetSocketAddress("192.168.137.61", 8888))
    val stream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_AND_DISK_SER_2)
    val lineDstream: DStream[String] = stream.map(x => new String(x.event.getBody.array()))
    val wordAndOne: DStream[(String, Int)] = lineDstream.flatMap(_.split(" ")).map((_, 1))
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
