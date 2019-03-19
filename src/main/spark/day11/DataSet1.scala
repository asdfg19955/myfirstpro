package day11

import org.apache.spark.sql.SparkSession

object DataSet1 {
  def main(args: Array[String]): Unit = {
    //1.只统计年龄在20以上的员工
    //2.根据部门join第二个文件
    //3.统计出每个部门分性别的平均薪资
    val spark = SparkSession.builder().appName("DataSet").master("local").getOrCreate()
    //导入隐式转换
    //导入sparkSQL中的内置函数
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val df1 = spark.read.json("D:\\ScalaAndSpark\\day12\\a.json")
    val df2=  spark.read.json("D:\\ScalaAndSpark\\day12\\b.json")
    //做第一个需求（DSL风格）
    df1.filter("age>20").join(df2,$"depId"===$"id")
    //根据部门和员工性别进行分组
      .groupBy(df2("name"),df1("gender"))
    //然后统计第三个指标
      .agg(avg(df1("salary"))).show()
  }
}
