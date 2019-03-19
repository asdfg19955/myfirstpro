package day14

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * receiver方式
  */
object KafkaReceiver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kafkawc").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(3))
    // 接下来编写kafka的配置信息
    // 首先先写zk的信息
    val zks = "192.168.203.22:2181,192.168.203.23:2181,192.168.203.24:2181"
    // kafka 消费组
    val groupId = "gp1"
    // 配置Kafka基本信息
    val topics = Map[String,Int]("test1"->1)
    // 创建Kafka的输入数据流，获取kafka中的数据
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zks,groupId,topics)
    // 获取到的数据格式 （key,value）
    data.map(t=>(t._1,t._2)).print()//这里面的flatMap的key代表偏移量

    ssc.start()
    ssc.awaitTermination()
  }
}
