package day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming_AccumulatorAndBroadcast {
  //1封装业务逻辑2创建对象
  def createContext(ip: String, port: Int, outputPath: String, checkpointDirectory: String) //接收四个参数
  : StreamingContext = {
    //如果没有打印出Creating new context 说明StreamingContext已经从checkpoint目录中创建
    println("Creating new context")
    val sparkConf = new SparkConf().setAppName("Streaming_AccumulatorAndBroadcast").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(15))
    ssc.checkpoint(checkpointDirectory)
    // 创建基于ip:port的Socket数据流，数据流中word以'\n'进行切分
    val lines = ssc.socketTextStream(ip, port)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _) //转换为二元组进行累加操作
    //eg: wordCounts = {RDD1={(hell0,2),(spark,1)},RDD2={(world,1),(spark,3)}...}
    wordCounts.print
    wordCounts.foreachRDD { rdd =>

      // 获取或注册黑名单广播变量blacklist Broadcast  Seq("hello", "world")
      val blacklist = WordBlacklist.getInstance(rdd.sparkContext) //rdd.sparkContext返回当前rdd中封装的SparkContext实例

      // 获取或注册删除单词累加器WordsCounter Accumulator  0
      val droppedWordsCounter = DroppedWrodsCounter.getInstance(rdd.sparkContext) //rdd.sparkContext返回当前rdd中封装的SparkContext实例

      // 使用黑名单blacklist过滤单词，并利用droppedWordsCounter对过滤掉的单词进行统计，并输出
      //      val counts = rdd.filter { case (word, count) =>
      val filteredrdd = rdd.filter { case (word, count) =>
        if (blacklist.value.contains(word)) { //判断是否包含"hello"或"world"
          droppedWordsCounter.add(count.toInt)
          println("the word: " + word + " is deleted " + count + " times")
          false //返回false值
        }
        else {
          true
        }

      }
      filteredrdd.saveAsTextFile(outputPath) //保存结果
      //      filteredrdd.foreach(println)  //输出
      println("the accumulator is " + droppedWordsCounter + "!!!!!!!!!!!!!") //返回累加器的总和

    }
    ssc //返回ssc
  }
  def main(args: Array[String]) {

    //
    //    if (args.length != 4) {
    //      System.err.println("Your arguments were " + args.mkString("[", ", ", "]"))
    //
    //      System.exit(1)
    //    }
    //
    //    //需要事先创建相应目录!!
    //    val Array(ip, port, outputPath, checkpointDirectory) = args
    val ssc = StreamingContext.getOrCreate("d:\\sparkstreamingtest1",
      () => createContext("192.168.137.51", 9999,"d:\\sparkstreamingtest2","d:\\sparkstreamingtest1"))
    ssc.start()
    ssc.awaitTermination()
  }
}
