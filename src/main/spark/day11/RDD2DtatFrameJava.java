package day11;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

/**
 * 第二种 编程接口的方式
 */
public class RDD2DtatFrameJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("rdd2DF").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        //创建一个普通RDD
        JavaRDD<String> rdd = jsc.textFile("D:\\ScalaAndSpark\\day11-Spark SQL\\students.txt");
        //这里注意一下，如果用反射的方式，那么类型一定是Row
        JavaRDD<Row> rowJavaRDD = rdd.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String[] split = v1.split(",");
                return RowFactory.create(
                        Integer.valueOf(split[0]),split[1],Integer.valueOf(split[2]));
            }
        });
        //接下来动态构造元数据
        ArrayList<StructField> fields  = new ArrayList<>();
        fields.add(DataTypes.createStructField("ids",DataTypes.IntegerType,true));
        fields.add(DataTypes.createStructField("names",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("ages",DataTypes.IntegerType,true));
       //构建structType
        StructType structType = DataTypes.createStructType(fields);
        //构建DF
       // DataFrame df = sqlContext.createDataFrame(rowJavaRDD, structType);
        //注册临时表
      // df.registerTempTable("stu");
        sqlContext.sql("select * from stu where ages < 18").show();


    }
}
