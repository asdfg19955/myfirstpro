package day09

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * 需求：根据访问日志的ip地址计算出访问者的归属地，并且按照省份，计算出访问次数，然后将计算好的结果写入到MySQL
  *1.整理数据，切分出ip字段，然后将ip地址转换成十进制
  *2.加载规则，整理规则，取出有用的字段
  *3.将访问log与ip规则进行匹配（二分法查找）
  *4.取出对应的省份名称，然后将其和一组合在一起
  *5.按省份名进行聚合
  *6.将聚合后的数据写入到MySQL中
  */
object IpLocation {

def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setAppName("IP").setMaster("local")
  val sc = new SparkContext(conf)
  //对这个文件进行取值(取ip的十进制和省份
  val lines=Source.fromFile(args(1)).getLines()
  val arr: Array[(Long, Long, String)] = lines.map(t => {
    val strings: Array[String] = t.split("[|]")
    val startNum: Long = strings(2).toLong
    val endNum: Long = strings(3).toLong
    val province: String = strings(6)
    (startNum, endNum, province)
  }).toArray

  //将IP地址解析数据进行广播，让每个worker都拥有一份
  val arrExecutor: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(arr)
  val value: Array[(Long, Long, String)] = arrExecutor.value
  //读取访问日志
  val accessLines: RDD[String] = sc.textFile(args(0))
  //进行切割，获得需要的数据,获取ip的十进制
  val ipNum= accessLines.map(t=>{
    //切分出IP
    val Strings: Array[String] = t.split("[|]")
    val ip: String = Strings(1)
    //将IP转换为10进制
    val ipNum: Long = MyUtils.ip2Long(ip)
    ipNum
  })
  val pp=ipNum.map(t=>{
    //利用二分法进行查找
    var province ="未知"
    val index: Int = MyUtils.binarySearch(arrExecutor.value,t)
    if (index != -1){
       province = value(index)._3
    }
    //返回最终结果数据
    (province,1)
  })
  //将数据进行聚合
  val reduce: RDD[(String, Int)] = pp.reduceByKey(_+_)
  reduce.foreach(println)


}
}
