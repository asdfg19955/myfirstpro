package day11



import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object UDFSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("udf").setMaster("local")
    val sc =new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //构造数据
    val names = Array("leo","Marry","Jack","Tom")
    val namesRDD= sc.parallelize(names)
    val rdd=namesRDD.map(t=>{
      Row(t)
    })
    val structType = StructType(Array(StructField("name",StringType,true)))
    val df = sQLContext.createDataFrame(rdd,structType)
    df.registerTempTable("names")

    sQLContext.udf.register("strLen",(str:String)=>str.length)
    sQLContext.sql("select name,strLen(name) from names").collect().foreach(println)
  }
}
