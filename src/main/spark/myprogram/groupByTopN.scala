package myprogram

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object groupByTopN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("groupByTopN").setMaster("local")
    val sc = new SparkContext(conf)
    val line = sc.textFile("D:\\ScalaAndSpark\\day08\\score.txt",2)
    val tuples=line.map(t=>{
      val s = t.split(" ")
      (s(0),s(1).toInt)
    })
    val sss = tuples.groupByKey()
     val reduce =sss.map(x=>{
       val clas= x._1
       val score: Array[Int] = x._2.toArray.sortWith(_>_).take(5)//先分组，通过groupBykey,然后组内排序
       (clas,score)
     })
    reduce.foreach(x=>{
      println(x._1)
      x._2.foreach(println)
      println("-------------------")
    })


  }

}
