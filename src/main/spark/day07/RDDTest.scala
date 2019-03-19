package day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //    // 如何并行化创建RDD
    //    val res0: RDD[Int] = sc.parallelize(Array(1,5,7,4,3))
    //    // 都* 乘以2 排序
    //    val res1: RDD[Int] = res0.map(_ * 2).sortBy(x=>x)
    //    // 过滤出大于等于5元素
    //    val res2: RDD[Int] = res1.filter(_ >= 5)

    //    val rdd1 = sc.parallelize(Array("a b c","d e f","h i j"))
    //    val rdd2: RDD[String] = rdd1.flatMap(_.split(" "))

//        val rdd1 = sc.parallelize(List(List("a b c"),List("d e f")))
//        val rdd2 = rdd1.flatMap(_.flatMap(_.split(" ")))

    //    val rdd1 = sc.parallelize(List(5,6,4,3))
    //    val rdd2 = sc.parallelize(List(1,2,3,4))
    //    val rdd3 = rdd1.union(rdd2) // 交集
    //    val rdd4 = rdd1.intersection(rdd2) // 并集
    //
    //    val rdd5 = rdd3.distinct()

        val rdd1 = sc.parallelize(List(("tom",1),("jerry",3),("Jack",2)))
        val rdd2 = sc.parallelize(List(("tom",5),("jerry",1),("shuke",2)))
//
// val res3 = rdd1.join(rdd2).collect().toBuffer

    val res3 = rdd1.leftOuterJoin(rdd2).collect().toBuffer
    println(res3)
//    val res3 = rdd1.rightOuterJoin(rdd2).collect().toBuffer

//        val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9),2)

//        val group: RDD[(Int, Iterable[Int])] = rdd1.map((_,1)).groupByKey()
//        group.foreach(println)
//       val reduce: RDD[(Int, Int)] = rdd1.map((_,1)).reduceByKey(_+_)
//          reduce.foreach(println)

       // println(rdd1.aggregate(5)(math.max(_,_) , _ + _))//分区聚合，总体聚合，分区聚合完以后是要加上初始值，总体聚合也要加初始值

       // val rdd1 = sc.parallelize(List("a","b","c","d","e","f"),2)
    //
       // println(rdd1.aggregate("|")(_ + _, _ + _))//谁先计算完就先返回给谁，前面是每个分区进行聚合，后面是总体进行聚合，

//    //
//        val rdd1 = sc.parallelize(List(("cat",2),("dog",12),("cat",12),("pig",30),("pig",23)),2)
//        rdd1.aggregateByKey(100)(_ + _, _ + _).foreach(println)//用tuple元组形式存储数据的时候，会有ByKey的算子??

  }
}
