package day11

import org.apache.spark.sql.{Dataset, SparkSession}

object DataSet2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ds").master("local").getOrCreate()
     val df = spark.read.json("")
    //缓存级别为内存加磁盘(RDD是内存)
     df.persist()
    //在这里需要创建临时表(2.0以后叫临时视图)
   /* df.createTempView("tmp")
    spark.sql("select * from tmp where age > 30")*/
    import spark.implicits._
    val pro: Dataset[Pro] = df.as[Pro]
    println(pro.rdd.partitioner.size)
    pro.repartition(5)
  }
}
case class Pro(name:String,age:Long,depId:Long,gender:String,salary:Long)