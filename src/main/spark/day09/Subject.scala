package day09

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计所有用户对每个学科的各个模块的访问量，取 Top2
  */
object Subject {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Subject").setMaster("local")
    val sc = new SparkContext(conf)
    // 获取数据源
    val file = sc.textFile("D:\\ScalaAndSpark\\day09\\data\\subject\\access.txt")
    // 处理数据
    val tuples = file.map(t=>{
      val fields = t.split("\t")
      val url = fields(1)
      val host = new URL(url).getHost//直接获取http//后面的第一个
      val subject = host.split("\\.")(0)
      (subject,1)
    })
    // 聚合数据
    val reduce = tuples.reduceByKey(_+_)
    // 降序排序
    val sroted = reduce.sortBy(_._2,false)
    // 取TopN
    val takes = sroted.take(4)

    takes.toBuffer.foreach(println)
  }
}
