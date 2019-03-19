package day14;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;//这个应该就可以看做API 我让他生产数据,我通过这个API,按照上面进行操作,至于他内部的操作原理,我们不用关心,按照操作即可得到我们想要的结果

public class Producers {
    public static void main(String[] args) {
        //通过properties类进行配置文件的配置,这个就相当于买菜
        Properties prop = new Properties();
        //指定生产者端口和IP
        prop.put("metadata.broker.list", "192.168.203.21:9092");
        //进行序列化方式
        prop.put("serializer.class", "kafka.serializer.StringEncoder");
        //定义一个Prouducer配置文件API ,通过配置文件,我们可以得到什么想要的东西,就像做菜一样,需要食材,才可以得到能吃的,这个相当于洗菜
        ProducerConfig producerConfig = new ProducerConfig(prop);
        //创建生产者
        Producer<String, String> producer = new Producer<>(producerConfig);
        int i =0;
        while (true){
            //发送消息
            producer.send(new KeyedMessage<String, String>("test1","msg"+i));
            System.out.println(" "+i);
            i++;
        }
    }
}
