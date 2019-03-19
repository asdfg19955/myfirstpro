package day09

import java.sql.DriverManager

import scala.io.{BufferedSource, Source}

object MyUtils {

  // 算法解析Ip转换十进制
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  // 获取需要的起始Ip和结束Ip和地域分布
  def readRules(path: String): Array[(Long, Long, String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fileds = line.split("[|]")
      // 需要转义
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }

  def binarySearch(lines: Array[(Long, Long, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }
  // mysql的连接
  def reduce2Mysql(it:Iterator[(String,Int)]): Unit ={
    // 一个迭代器代表一个分区，分区中有多条数据
    // 先获得一个JDBC连接
    val connection = DriverManager
      .getConnection("jdbc:mysql://localhost:3306/spark-1","root","123456")
    // 将数据通过Connection写入数据库
    val pstm = connection.prepareStatement("insert into access_log values(?,?)")
    // 将分区中的数据每一条写入mysql中
    it.foreach(t=>{
      pstm.setString(1,t._1)
      pstm.setInt(2,t._2)
      pstm.executeUpdate()
    })
    // 将分区数据写入完成后，关闭连接
    if(pstm != null){
      pstm.close()
    }
    if(connection != null){
      connection.close()
    }
  }

}