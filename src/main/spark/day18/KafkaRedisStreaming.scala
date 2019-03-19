package day18

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream

import org.apache.spark.streaming.{Seconds, StreamingContext}

///**
//  * 直连方式，将offset保存到redis中
//  */
//object KafkaRedisStreaming {
//  // 过滤日志
//  Logger.getLogger("org").setLevel(Level.WARN)
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("offserAndredis").setMaster("local[2]")
//         // 每秒钟每个分区kafka拉取数据消息的速率
//      .set("spark.streaming.kafka.maxRatePerPartition","100")
//         // 设置序列化
//      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    val ssc = new StreamingContext(conf,Seconds(3))
//    // 设置消费者组
//    val groupId = "test2"
//    // kafka配置参数
//    val kafkas = Map[String,Object](
//      // 指定消费kafka的ip和端口
//      "bootstrap.servers" ->
//      "192.168.28.128:9092,192.168.28.129:9092,192.168.28.130:9092",
//    // 设置kafka的解码方式
//    "key.deserializer"->classOf[StringDeserializer],
//    "value.deserializer"->classOf[StringDeserializer],
//    "group.id"->groupId,
//    // 从头消费
//    "auto.offset.reset" -> "earliest",
//    "enable.auto.commit" -> (false:java.lang.Boolean)
//    )
//    // 将Topic放入数组
//    val topics = Array("test2")
//    // 获取redis中的kafka偏移量
//    var formdbOffset :Map[TopicPartition,Long] = JedisOffset(groupId)
//
//    // 拉取kafka数据
//    val stream:InputDStream[ConsumerRecord[String,String]] =
//    // 判断一下，我们要消费的kafka数据是否是第一次消费，之前有没有消费过
//      if(formdbOffset.size == 0){
//        KafkaUtils.createDirectStream[String,String](
//          ssc,
//          // 本地化策略
//          // 一般都都是这样写，它会将分区数据尽可能的均匀分布给可用的Executor。
//          LocationStrategies.PreferConsistent,
//          //消费者策略
//          // Subscribe: 不可以动态的更改消费的分区，一般都使用在开始读取数据的时候
//          // Assign: 它可以消费固定的topic的partition（集合）
//          // SubscribePattern: 可以用于在消费过程中增加分区
//          ConsumerStrategies.Subscribe[String,String](topics,kafkas)
//        )
//    }else{
//        // 如果不是第一次消费数据
//        KafkaUtils.createDirectStream(
//          ssc,
//          LocationStrategies.PreferConsistent,
//          ConsumerStrategies.Assign[String,String](
//            formdbOffset.keys,kafkas,formdbOffset)
//        )
//      }
//    // 处理数据
//    stream.foreachRDD(rdd=>{
//      //首先我们想获取处理数据的全信息，包括topic partition、offset
//      val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      // 业务处理
//      rdd.map(_.value()).foreach(println)
//      // 更新偏移量
//      val jedis = JedisConnectionPool.getConnection()
//      // 获取offset信息
//      for(or <- offsetRange){
//        jedis.hset(groupId,or.topic+"-"+or.partition,or.untilOffset.toString)
//      }
//      jedis.close()
//    })
//    // 启动程序
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
