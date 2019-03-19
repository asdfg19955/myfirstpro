//package com.day18
//
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
//import org.I0Itec.zkclient.ZkClient
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
//import org.apache.spark.streaming.{Duration, StreamingContext}
//
///**
//  * 直连方式（重点）
//  */
//object KafkaDirect {
//  def main(args: Array[String]): Unit = {
//    // spark Streaming
//    val conf = new SparkConf().setAppName("kdw").setMaster("local[2]")
//    val ssc = new StreamingContext(conf,Duration(5000))
//    // 指定组名
//    val group = "g001"
//    // 指定topic
//    val topic = "test1"
//    // 指定Kafka的broker地址（SparkStreaming的程序，在消费数据的时候，直连到Kafka的分区上，而不去连接zk）
//    val brokerList = "192.168.28.128:9092,192.168.28.129:9092,192.168.28.130:9092"
//    // 指定连接ZK的地址，后期更新偏移量时使用，这里和之前的receiver方式连接ZK不一样了，这里是手动保存偏移量
//    val zks = "192.168.28.131:2181,192.168.28.131:2182,192.168.28.131:2183"
//    // 创建topic的集合，也就是说可能消费多个Topic
//    val topics = Set(topic)
//    // 准备Kafka的配置参数
//    val kafkas = Map(
//      "metadata.broker.list"->brokerList,
//      "group.id"->group,
//      "auto.offset.reset"->kafka.api.OffsetRequest.SmallestTimeString
//    )
//    // 开始做offset保存前期工作（手动维护offset）
//    // 用于指定往这块中写入数据的目录，用于保存偏移量
//    val topicDirs = new ZKGroupTopicDirs(group,topic)
//    // 获取 zookeeper中的路径 "/consumers/g001/offsets/test1"
//    val zkTopicPath: String = s"${topicDirs.consumerOffsetDir}"
//    // 首先获取zkClient 可以获取读取偏移量数据，并更新偏移量
//    val zkClient: ZkClient = new ZkClient(zks)
//    // 查询该路径下是否有子目录 返回值是Int类型
//    val client: Int = zkClient.countChildren(zkTopicPath)
//    // 创建DStream
//    var kafkaStream :InputDStream[(String,String)] = null
//    // 如果zookeeper中保存offset，我们会用offset作为起始位置
//    // TopicAndPartition offset
//    var fromOffsets:Map[TopicAndPartition,Long] =Map()
//    // 如果我们zk有保存过offset
//    if(client > 0){
//      // client 分区的数量
//      for(i <- 0 until client){
//        // "/consumers/g001/offsets/test1/1"
//        // "/consumers/g001/offsets/test1/2"
//        // "/consumers/g001/offsets/test1/3"
//        // offset取值
//        val partitionOffset:String = zkClient.readData(s"${zkTopicPath}/${i}")
//        // 将不同partition对应的offset增加到fromOffset中
//        val topicAndPartition = TopicAndPartition(topic,i)
//        // 将数据加入进去
//        fromOffsets +=(topicAndPartition->partitionOffset.toLong)
//      }
//      // 创建需要的函数
//      // key:kafka的Key，value：“123456789”
//      // 这个会将kafka的消息进行转换，最终kafka的数据都会变成Key，message这样的Tuple
//      val messageHandler = (mmd:MessageAndMetadata[String,String])=>{(mmd.key(),mmd.message())}
//      // 读取kafka数据
//      // [String,String,StringDecoder,StringDecoder,(String,String)]
//      //  key     value    key的解码方式  value的解码方式
//      kafkaStream = KafkaUtils.createDirectStream
//        [String,String,StringDecoder,StringDecoder,(String,String)](
//          ssc,kafkas,fromOffsets,messageHandler)
//    }else{
//      // 如果未保存，根据kafka的配置从最开始的位置读取数据也就是offset
//      kafkaStream = KafkaUtils.createDirectStream
//        [String,String,StringDecoder,StringDecoder](ssc,kafkas,topics)
//    }
//    // offset的范围
//    var offsetRanges = Array[OffsetRange]()
//    // 处理数据
////    val transfrom = kafkaStream.transform(rdd=>{
////      // 先进行offset处理
////      // 首先得到是kafka的消息offset
////      // 获取此offset，方便后面进行更新offset
////      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
////      rdd
////    })
//    // 在进行业务处理
//    kafkaStream.foreachRDD { rdd =>
//      if(!rdd.isEmpty()){
//        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//        // 对RDD数据进行操作
//        val lines = rdd.map(_._2).filter(_.split(" ").length>=5).map(_.split(" "))
//        // 问题1.计算出总的成交量总额（结果保存到redis中）
//        //RUtils.moneyCount(lines)
//        // 问题2.计算每个商品分类的成交量（结果保存到redis中）
//        RUtils.Count(lines)
//      }
//      // 更新偏移量
//      for (o <- offsetRanges){
//        // 取值 (路径)
//        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
//        // 将该partition的offset保存到zk中
//        ZkUtils.updatePersistentPath(zkClient,zkPath,o.untilOffset.toString)
//      }
//    }
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
