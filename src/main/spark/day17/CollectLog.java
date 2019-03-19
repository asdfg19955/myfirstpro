package day17;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class CollectLog {
   public static void main(String[] args){
     Properties properties = new Properties();
     properties.setProperty("metadata.broker.list",
             "192.168.28.128:9092,192.168.28.129:9092,192.168.28.130:9092");
     //消息传递到broker时的序列化方式
      properties.setProperty("serializer.class",StringEncoder.class.getName());
      //zk的地址
      properties.setProperty("zookeeper.connect",
              "192.168.14.131:2181,192.168.14.131:2182,192.168.14.131:2183");
      //是否反馈消息 0是不反馈消息 1是反馈消息
      properties.setProperty("request.required.acks","1");
      ProducerConfig producerConfig = new ProducerConfig(properties);
      Producer<String,String> producer = new Producer<String,String>(producerConfig);
      try {
         BufferedReader bf = new BufferedReader(
                 new FileReader(
                         new File(
                                 "D://文件路径")));
         String line = null;
         while((line=bf.readLine())!=null){
             KeyedMessage<String,String> keyedMessage = new KeyedMessage<String,String>("test1",line);
           Thread.sleep(1000);
            producer.send(keyedMessage);
         }
         bf.close();
         producer.close();
         System.out.println("已经发送完毕");
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}
