package com.day03

object ScalaWC {
  def main(args: Array[String]): Unit = {
    //创建list
    val lines = List("hello tom hello jarry","hello suke hello bata","hello jack")
     //把数据切分压平
    val words = lines.flatMap(_.split(" "))
    //过滤空格
    val filtered = words.filter(_ != "")
    //生成元组形式(word,1)
   val tuples: List[(String, Int)] = filtered.map((_,1))
    //以key进行分组
    val stringToTuples: Map[String, List[(String, Int)]] = tuples.groupBy(x=>x._1)
    //开始统计分组后相同的单词个数
    //val stringToInt: Map[String, Int] = stringToTuples.map(x=>(x._1,x._2.size))
    val stringToInt: Map[String, Int] = stringToTuples.mapValues(_.size)
    val ss = stringToInt.toList.sortBy(-_._2)
    ss.foreach(println)
  }
}
