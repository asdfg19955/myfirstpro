package myprogram


import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object CountLoad {
  def main(args: Array[String]): Unit = {
      val conf: SparkConf = new SparkConf().setAppName("load").setMaster("local")
      val context = new SparkContext(conf)
      val sqlContext: SQLContext = new SQLContext(context)
      val df: DataFrame = sqlContext.read.json("D:\\QQ\\qqFile\\1223137801\\FileRecv\\JsonTest02.json")
      //    df.show(20)
      //df.select("terminal").filter("status = 1").groupBy("terminal").count().show()
      df.select("phoneNum","province").filter("status = 1").groupBy("phoneNum","province").count().sort().show(2)





  }

}
