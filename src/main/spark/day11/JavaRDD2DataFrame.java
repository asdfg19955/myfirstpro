package day11;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;


/**
 * 实现RDD转换DF(反射的方式创建DF)
 */
public class JavaRDD2DataFrame {
    public static void main(String[] args) {
        // 创建一个普通的RDD
        SparkConf conf = new SparkConf().setAppName("JavaRdd2DF").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        JavaRDD<String> rdd = jsc.textFile("D:\\hzbigdata02\\day11-Spark SQL\\students.txt");
        // 切分数据
        JavaRDD<student> students = rdd.map(new Function<String, student>() {
            @Override
            public student call(String v1) throws Exception {
                String[] split = v1.split(",");
                student stu = new student();
                stu.setId(Integer.valueOf(split[0]));
                stu.setAge(Integer.valueOf(split[2]));
                stu.setName(split[1]);
                return stu;
            }
        });
        // 使用反射方式，将RDD转换为DF
        // 首先传入一个RDD ，然后在传入一个类，这个类需要通过反射的原理进行传入
       // DataFrame df = sqlContext.createDataFrame(students, student.class);
        // 我们拿到DF之后，就可以注册一个临时表，然后针对这个表中的数据执行SQL
        //df.registerTempTable("stu");
        // 针对这边表进行查询 查询年龄小于18的学生
        //DataFrame sql = sqlContext.sql("select * from stu where age < 18");
       // sql.show();
        // 将查询出来的数据，再次转换成RDD处理，再次处理的时候注意个RDD的类型，是Row
        //JavaRDD<Row> rowJavaRDD = sql.javaRDD();
        // 调用Map算子，进行处理，将数据存入实体类中，然后在取出来展示
        //JavaRDD<student> studentJavaRDD = rowJavaRDD.map(new Function<Row, student>() {
           /* @Override
            public student call(Row v1) throws Exception {
                student stu = new student();
                stu.setId(v1.getInt(1));
                stu.setAge(v1.getInt(0));
                stu.setName(v1.getString(2));
                return stu;
            }
        });*/
        // 将数据收回来，打印输出
       /* List<student> list = studentJavaRDD.collect();
        for (student stu:list){
            System.out.println(stu.getId() + "   " + stu.getAge() + "   " + stu.getName());
        }*/
    }
}
