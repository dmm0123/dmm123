package day02

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
 * 注册广播变量
 */
object WordBlacklist {
private var instance:Broadcast[Seq[String]]=null
  def getInstance(sc:SparkContext):Broadcast[Seq[String]]={
    if(instance ==null){
      synchronized{
        if(instance ==null){
          val wordBlacklist = Seq("hello","world")
          instance = sc.broadcast(wordBlacklist)
        }
      }
    }
    instance
  }
}
