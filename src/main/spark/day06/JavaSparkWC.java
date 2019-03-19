package day06;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;



public class JavaSparkWC {
    public static void main(String[] args) {
        //创建配置文件对象
        SparkConf conf = new SparkConf().setAppName("JavaSparkWC").setMaster("local");
        //创建Spark的上下文对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //读取数据
        JavaRDD<String> file = sc.textFile("D:\\wc.txt");
        //进行单词统计
        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split("")).iterator();
            }
        });
        //返回一个元组
        JavaPairRDD<String, Integer> tuple = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> reduce= tuple.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(reduce.collect());
    }

}

