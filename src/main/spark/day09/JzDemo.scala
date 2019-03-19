package day09

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：在一定时间方位内，求出用户在所有基站停留的时长
  */
object JzDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir",
      "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    val conf = new SparkConf().setAppName("jz").setMaster("local")
    val sc = new SparkContext(conf)
    //获取用户访问基站信息数据
    val files = sc.textFile("D:\\hzbigdata02\\day09\\data\\mobilelocation\\log")
    // 切分数据
    val fields = files.map(line=>{
      // 切分
      val fields = line.split(",")
      val phone = fields(0)// 手机号
      val time = fields(1).toLong// 时间戳
      val lac = fields(2) // 基站ID
      val eventType = fields(3).toInt // 事件类型
      val time_long = if(eventType == 1) -time else time
      ((phone,lac),time_long)
    })
    // 用户在相同基站停留的时间的总和
    val sumedPhoneAndLacAndTime: RDD[((String, String), Long)] = fields.reduceByKey(_+_)
    // 为了方便join 需要整理用户在基站的停留时间信息，把lac放到Key的位置
    val lacAdnPhoneAndTime = sumedPhoneAndLacAndTime.map(t=>{
      val phone = t._1._1// 手机号
      val lac = t._1._2 // 基站
      val time = t._2   // 用户在单个基站停留的总时长
      (lac,(phone,time))
    })
    // 读取基站信息
    val lacInfo = sc.textFile("D:\\hzbigdata02\\day09\\data\\mobilelocation\\lac_info.txt")
    // 处理基站数据
    val lacAndXY = lacInfo.map(t=>{
      val fields = t.split(",")
      val lac = fields(0)// 基站ID
      val x = fields(1) //经度
      val y = fields(2) //纬度
      (lac,(x,y))
    })
    // 将用户信息和基站信息通过Key进行Join
    val joined: RDD[(String, ((String, Long), (String, String)))] = lacAdnPhoneAndTime.join(lacAndXY)
    //joined.foreach(println)
    // 把数据整合一下
    val PhoneAndTimeAndXY = joined.map(t=>{
      val lac = t._1 // 基站
      val phone = t._2._1._1 // 手机
      val time = t._2._1._2   // 时间
      val xy = t._2._2 // 经纬度
      (phone,time,xy)
    })
    // 按照手机进行分组
    val grouped: RDD[(String, Iterable[(String, Long, (String, String))])] =
      PhoneAndTimeAndXY.groupBy(_._1)
    // 按照时间进行排序
    val sorted = grouped.mapValues(_.toList.sortBy(_._2).reverse)
    sorted.foreach(println)
    sc.stop()
  }
}
