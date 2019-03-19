package day08

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 累加器
  */
object Accumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Acc").setMaster("local")
    val sc = new SparkContext(conf)
    // 创建一个累加器
    var sum = sc.accumulator(1)//将累加器初始化
    val par = sc.parallelize(Array(1,2,3)).foreach(x=>{
      sum += x
    })
    println(sum)
  }
}
