package day11


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * scala编程接口的方式
  */

object RDD2DataFrameScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Rdd2DF").setMaster("local")
    val context = new SparkContext(conf)
    val sQLContext = new SQLContext(context)
    val rdd: RDD[String] = context.textFile("D:\\ScalaAndSpark\\day11-Spark SQL\\students.txt")
    val stuRdd=rdd.map(t=>{
      val s = t.split(",")
      Row(s(0).toInt,s(1),s(2).toInt)
    })
    //构建structType
    val structType = StructType(Array(StructField("id",IntegerType,true),StructField("name",StringType,true),
      StructField("age",IntegerType,true)))
    //构建DF
    val df = sQLContext.createDataFrame(stuRdd,structType)
    //创建临时表
    df.registerTempTable("stu")
    //写sql
    val df2 = sQLContext.sql("select * from stu where age >15")
    df2.rdd.foreach(println)
  }
 

}


