package myprogram


import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.mutable

object SougouResult {
 def main(args: Array[String]): Unit = {
     val spark = SparkSession.builder().appName("SougouLog").master("local").getOrCreate()
    //导入隐式转换
    import spark.implicits._
    //导入SparkSQL的内置函数
//    import org.apache.spark.sql.functions._
    //导入数据
     val s1:RDD[String] = spark.sparkContext.textFile("D:\\ScalaAndSpark\\day12\\ID.txt")
     val s2:RDD[String] = spark.sparkContext.textFile("D:\\ScalaAndSpark\\day12\\SogouQ.txt")

     val rdd1 = s1.map(t=>{

      val map1:mutable.Map[String,String] = new mutable.HashMap[String,String]()
      val str = t.split("\\s+")
      map1.put(str(0),str(1))
      map1
    }).collect().flatten.toMap
    //将小文件广播出去

    val s = spark.sparkContext.broadcast(rdd1)

    val rdd=s2.map(v=>{
      val s = v.split("\\s+")
      (s(0).split(":")(0),s(4))
    })

    val rdd2 = rdd.map(x=>{
      val key = x._2
      val str = s.value.getOrElse(key,"其他")
      news(x._1,str)
    })


   spark.createDataFrame(rdd2).createOrReplaceTempView("souGou")
  //spark.sql("select time, ty ,count(ty) from souGou group by time, ty order by time").show(7000)
   val dataFrame = spark.sql("select time, ty ,count(*) from souGou group by time, ty order by time")
   val properties = new Properties()
   properties.setProperty("user","root")
   properties.setProperty("password","123")
   dataFrame.write.mode("append").jdbc("jdbc:mysql://localhost:3306/test","sougou",properties )


//   val df1 = rdd2.toDF()
//   df1.show()
////   df1.foreach(r=>{
////     println(r.mkString)
////   })
//   df1.registerTempTable("table1")
////  spark.sql("select ty,time  from table1").show()
//
  }



}

case class news(time:String,ty:String)