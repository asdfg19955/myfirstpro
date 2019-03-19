package day11

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameCreate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //读取文件
    val df: DataFrame = sqlContext.read.json("D:\\ScalaAndSpark\\day11-Spark SQL\\students.json")
    //查看文件内容
    //只显示前20条
    //df.show(2)
    //df.printSchema() 显示元数据信息
    //println(df.collect().toBuffer) 返回一个数组
    //df.describe("age","name").show() 可以计算出Count，平均值，差值，最大最小值
    //println(df.first()) 取出第一行，但是也是按照字典排序。
   // df.where("id = 1 or name ='leo'").show()
    //df.filter("id = 1 or name ='leo'").show()
    //df.select("id ").show() 查询字段的
    //df.sort(-df.col("age")).show(false) 获取指定字段，主要是结合方法使用
   // df.drop(df.col("id")).show() 去除指定的字段
    //println(df.groupBy("id").toString) 分组
    //df.distinct().show() 去重
    //df.dropDuplicates(Array("id")).show() 按照指定字段去重
    //df.select(df.col("name"),df.col("age").plus(2)).show() 查询某一列的值，对某一列进行计算
    //df.filter(df.col("age").gt(18)).show() 对age进行过滤
    //df.groupBy(df.col("age")).count().show() 根据某一列进行分组，然后求count
    df.registerTempTable("stu")
    sqlContext.sql("select sum(age) as total from stu").show()

}

}
