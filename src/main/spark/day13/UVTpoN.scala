package day13

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 案例：统计每天每个搜索词用户访问量
  * 1.使用fitler算子进行过滤(有过滤条件)
  * 2.将数据转换为规定格式，对它进行分组，再去重
  * 3.统计去重后的UV
  * 4.最后输出的结果格式
  */
object UVTpoN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("uvTopN").setMaster("local")
    val sc   = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //导入隐式转换
    import sQLContext.implicits._
    //伪造需要查询到的数据，查询条件
    var queryMap = Map(
      "city"->List("beijing"),
      "platform"->List("android"),
      "version"->List("1.0","1.1","1.2","1.3")
    )
    //将查询条件封装为一个BradCast
    val queryMapCast = sc.broadcast(queryMap)
    //读取数据源
    sc.textFile("")
  }
}
