package day01
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
object SparkKafaka02 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf =new SparkConf().setMaster("local[2]").setAppName("SparkKafaka02")
    val ssc = new StreamingContext(conf,Seconds(2))
    // 主题配置

    // 配置kafka参数。使用map集合。
    val kafkaParmas = Map(
      "metadata.broker.list" -> "192.168.137.61:9092,192.168.137.62:9092,192.168.137.63:9092  ",
      "group.id" -> "g1")
    val topics = Set("first3","first5")
    // 使用kafkaUtil。当做数据源获取数据  （直连方式）
    val streams: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParmas,topics)
    //    var zkServer = "192.168.137.8:2181,192.168.137.9:2181,192.168.137.10:2181,"
    //    val topics1 = Map[String,Int]("kafka_spark1"->1)
    // 只用kafkaUtil 当做数据源读取数据（接收器模式）
    //    val line1: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkServer,"p1",topics1)

    val line: DStream[String] = streams.map(_._2)

    val reduced: DStream[(String, Int)] = line.flatMap(_.split(" ").map((_,1))).reduceByKey(_+_)
    reduced.print()

    ssc.start()

    ssc.awaitTermination()



  }

}