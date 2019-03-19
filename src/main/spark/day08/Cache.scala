package day08

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 持久化RDD
  */
object Cache {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("cache").setMaster("local")
    val sc = new SparkContext(conf)
    // cache或者persist 但是使用起来要注意几点
    // 你必须要先读取数据源，在做缓存
    // 选择cache或persist，根据你的业务和机器的资源来

    val lines = sc.textFile("D:\\hello.txt").persist(StorageLevel.DISK_ONLY_2)

    println(lines.count())

  }
}
