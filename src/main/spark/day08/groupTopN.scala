package day08

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 分组取 TopN
  */
object groupTopN {
  def main(args: Array[String]): Unit = {

     val conf = new SparkConf().setAppName("groupTopN").setMaster("local")
    val sc = new SparkContext(conf)
    // 读取文件
     val file = sc.textFile("D:\\hzbigdata02\\day08\\score.txt",5)
    // 先进行分组，然后对组内进行排序
    val tuples = file.map(t=>{
      val classed = t.split(" ")
      (classed(0),classed(1).toInt)
    })
    val grouped: RDD[(String, Iterable[Int])] = tuples.groupByKey()
    val result = grouped.map(t=>{
      val classs = t._1
      val score = t._2.toArray.sortWith(_ > _).take(3)
      (classs,score)
    })
    result.foreach(t=>{
      println(t._1)
      t._2.foreach(println)
      println("---------------------")
    })
  }
}
