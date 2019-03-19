package day06

import org.apache.spark.{SparkConf, SparkContext}

object ScalaWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("scalcWC").setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile("D:\\wc.txt")
    val line =file.flatMap(t=>{
      val s = t.split(" ")
       s
    })
    val tuple =line.map(x=>{
      (x,1)
    })
    val reduce = tuple.reduceByKey(_+_)
    println(reduce.sortByKey(false).collect().toBuffer)

  }


}
