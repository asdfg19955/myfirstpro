package day08

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广播变量
  */
object Broadcast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("broadcast").setMaster("local")
    val sc = new SparkContext(conf)
    val factor = 3
    // 把factor做成广播变量，让每个节点只有一份
    val broad = sc.broadcast(factor)
    val rdd = sc.parallelize(Array(1,2,3,4,5),2)
    // 将广播变量引用
    val maps = rdd.map(_ * broad.value)
    maps.foreach(println)
  }
}
