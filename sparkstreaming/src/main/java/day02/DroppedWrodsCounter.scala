package day02

/**
 * 累加器
 */

import org.apache.spark.{Accumulator, SparkContext}
object DroppedWrodsCounter {
private var instance:Accumulator[Int]=null
  def getInstance(sc:SparkContext):Accumulator[Int]={
    if(instance == null){
      synchronized{
        if(instance == null){
          instance = sc.accumulator(0)
        }
      }
    }
    instance
  }
}
