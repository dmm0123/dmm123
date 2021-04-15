package day01

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext, dstream}

object SparkKafka01 {
  def main(args: Array[String]): Unit = {
    //创建SparkConf对象
    val conf: SparkConf = new SparkConf()
      .setAppName("SparkKafka01")
      .setMaster("local[2]")
    // 2.创建SparkContext对象
    val sc: SparkContext = new SparkContext(conf)
    //创建StreamingContext
    /**
     * 参数说明：
     * 参数一：SparkContext对象
     * 参数二：每个批次的间隔时间
     */
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    //通过kafkaUtils.createDirectStream 对接kafka采用是kafka低级api偏移量不受zk管理
    //配置kafka相关参数
    val kafkaParams = Map(
      "metadata.broker.list" -> "192.168.137.61:9092,192.168.137.62:9092,192.168.137.63:9092",
      "group.id" -> "g1")
    // 4.2.定义topic
    val topics = Set("first3")

    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)

    //.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    // 5.获取topic中的数据

    val topicData: DStream[String] = dstream.map(_._2)
    // 6.切分每一行,每个单词计为1
    val wordAndOne: DStream[(String, Int)] = topicData.flatMap(_.split(" ")).map((_, 1))

    // 7.相同单词出现的次数累加
    val resultDS: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    // 8.通过Output Operations操作打印数据
    resultDS.print()

    // 9.开启流式计算
    ssc.start()

    // 阻塞一直运行
    ssc.awaitTermination()

  }
}
