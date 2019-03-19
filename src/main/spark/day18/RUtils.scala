package day18

import org.apache.spark.rdd.RDD

object RUtils {

  // 计算总额
  def moneyCount(liens :RDD[Array[String]]): Unit ={
    // 根据数据取值
    val money = liens.map(arr=>{
      arr(4).toDouble
    })
    // 这里执行的是在Driver端，所以不要轻易使用collect算子
    val count = money.reduce(_+_)
    // 得到最终结果数据
    val jedis = JedisConnectionPool.getConnection()
    jedis.incrByFloat("MoneyCount",count)
    jedis.close()
  }
  // 计算分类成交量
  def Count(liens :RDD[Array[String]]): Unit ={
    // 根据字段取值
    val reduce = liens.map(arr=>{
      (arr(2),1.toLong)
    }).reduceByKey(_+_)
    // 进行写入redis的时候，注意，不能直接foreach
    reduce.foreachPartition(f=>{
      val jedis = JedisConnectionPool.getConnection()
      f.foreach(t=>{
        jedis.incrBy(t._1,t._2)
      })
      jedis.close()
    })
  }
}
