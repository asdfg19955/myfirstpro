package day08

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对文件取 TopN
  */
object TopN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TopN").setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile("D:\\ScalaAndSpark\\day08\\top.txt")
    val tuples = file.map(t=>(Integer.valueOf(t),t))
    val topN = tuples.sortByKey(false).take(3)
    topN.foreach(t=>t._2)
    sc.stop()
  }
}