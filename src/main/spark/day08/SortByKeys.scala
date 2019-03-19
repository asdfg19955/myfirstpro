package day08

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 二次排序
  */
case class SortByKeys(val first:Int,val second:Int)extends Ordered[SortByKeys]{
  override def compare(that: SortByKeys): Int = {
    if(this.first - that.first != 0) {
      this.first -that.first
    }else{
      this.second - that.second
    }
  }
}

object SortByKeys{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sort").setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile("D:\\ScalaAndSpark\\day08\\sort.txt")

    val tuples = file.map(t=>{
      (SortByKeys(t.split(" ")(0).toInt,t.split(" ")(1).toInt),t)
    })

    val sorted = tuples.sortByKey()

    sorted.foreach(println)
  }
}