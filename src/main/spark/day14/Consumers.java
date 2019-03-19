package day14;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 消费者
 */
public class Consumers {
    public static void main(String[] args) {
        // 配置相应的属性
        Properties prop = new Properties();
        // 配置基本信息
        prop.put("zookeeper.connect","192.168.203.22:2181,192.168.203.23:2181,192.168.203.24:2181");
        // 配置消费者组
        prop.put("group.id","vvv");
        // 将配置信息加载
        String topics = "test1";
        ConsumerConfig config = new ConsumerConfig(prop);
        // 创建消费者
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
        // 创建Map，主要用来存储多个Topic信息
        Map<String, Integer> hashMap = new HashMap<>();
        hashMap.put(topics,2);
        // 获取信息流
        Map<String, List<KafkaStream<byte[], byte[]>>> msg = consumer.createMessageStreams(hashMap);
        List<KafkaStream<byte[], byte[]>> kafkaStreams = msg.get(topics);
        // 循环接收数据流信息
        for (KafkaStream<byte[], byte[]> stream:kafkaStreams){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (MessageAndMetadata<byte[] , byte[]> m : stream){
                        String msg = new String(m.message());
                        System.out.println(msg);
                    }
                }
            }).start();
        }
    }
}
