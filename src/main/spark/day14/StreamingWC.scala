package day14

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 入门
  */
object StreamingWC {
  def main(args: Array[String]): Unit = {
    // 创建执行入口
    val conf = new SparkConf().setAppName("wc").setMaster("local[2]")//需要两个线程,因为一个线程需要拉取数据,另一个需要处理数据,一开始只给一个的话,就会出现生成空缺,跑离线可以的,实时执行程序,最少两个线程
    // 设置批次提交的时间
    val ssc = new StreamingContext(conf,Seconds(5))
    // 首先先创建一个数据源，也就是数据流，先拿到一个DStream
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.203.21",6666)
    // 接下来 开始接收数据，并执行计算，使用SparkCore算子，执行操作在DStream中完成
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val tuple = words.map((_,1))
    val result = tuple.reduceByKey(_+_)
    // 统计一些结果数据
    result.print()
    // 启动程序
    ssc.start()
    // 等待停止
    ssc.awaitTermination()
  }
}
