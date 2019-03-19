package day11


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * scala版本的反射方式
  */
object RDD2DataFrame {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("rdd2DF").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val rdd: RDD[String] = sc.textFile("D:\\ScalaAndSpark\\day11-Spark SQL\\students.txt")
    val stuRdd =rdd.map(t=>{
       val s = t.split(",")
      Stu(s(0).toInt,s(1),s(2).toInt)
    })
    //创建DF
    //需要导入隐式转换
    import sqlContext.implicits._
    val df = stuRdd.toDF()
    //df.show()
    //注册临时表
    df.registerTempTable("stu")
    //通过这个表名进行查找
    val df2 = sqlContext.sql("select * from stu where age < 18")
    //将这个表进行业务处理
    df2.write.format("text").save("D:\\stu")


  }

}
case class Stu(id:Int,name:String,age:Int)
