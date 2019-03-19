package day11


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object FilterData {
  def main(args: Array[String]): Unit = {
            val conf = new SparkConf().setAppName("FilterData").setMaster("local")
            val context = new SparkContext(conf)
            val sQLContext = new SQLContext(context)
    val userSaleLog = Array(
      "2018-1-01,55.05,110",
      "2018-1-01,23.15,113",
      "2018-1-01,15.20",
      "2018-1-02,56.05,114",
      "2018-1-02,78.87,115",
      "2018-1-02,113.02,112")

    val rdd1: RDD[String] = context.parallelize(userSaleLog)

    val rowRDD =rdd1.filter(t => {
      if (t.split(",").length >=3) true else false
    }).map(row=>{
      Row(row.split(",")(0),row.split(",")(1))

    })
    //构建StructType
    val structType = StructType(Array(StructField("date",StringType,true),StructField("money",DoubleType,true)))

    //构建DF
     val df: DataFrame = sQLContext.createDataFrame(rowRDD,structType)
    import sQLContext.implicits._
    import org.apache.spark.sql.functions._

  }
}
