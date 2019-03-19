package day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * map
  * filter
  * flatMap
  * groupByKey
  * reduceByKey
  * sortBy
  * sortByKey
  * join
  * cogroup
  */
object Transformation {
  def main(args: Array[String]): Unit = {
    //map()
    //groupByKey()
    //cogroup()
//    Mappartitions()
   // Distinct
    //Coalesce
   // join()
   Coalesce

      }
  def map(): Unit ={
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(1,2,3,4,5)
    val rdd1 = sc.parallelize(arr)
    rdd1.map(x=>x * 2).foreach(println)
  }
  def filter(): Unit ={
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(1,2,3,4,5)
    val rdd1 = sc.parallelize(arr)
    rdd1.filter(x =>x % 2 == 0).foreach(println)
  }
  def flatMap(): Unit ={
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array("hello you","hello me","hello world")
    val rdd1 = sc.parallelize(arr)
    rdd1.flatMap(_.split(" ")).foreach(println)
  }
  def groupByKey(): Unit ={
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(("class",80),("class2",98),("class",99))
    val rdd1 = sc.parallelize(arr)
    val classCores: RDD[(String, Iterable[Int])] = rdd1.groupByKey()
    classCores.foreach(println)
    classCores.foreach(x=>{
      println(x._1)
      x._2.foreach(println)
    })
  }
  def reduceByKey(): Unit ={
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(("class",80),("class2",98),("class",99))
    val rdd1 = sc.parallelize(arr)
    rdd1.foreach(println)
    val classCores = rdd1.reduceByKey(_+_)
    classCores.foreach(println)
  }
  def sortByKey(): Unit ={
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(("class",80),("class2",98),("class",99))
    val rdd1 = sc.parallelize(arr)

  }

  def join(): Unit ={
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val studentList = Array(
      (1,"xiaohong"),
      (2,"xiaoli"),
      (3,"xiaohua"),
      (7,"zhuhzhu")
    )
    val ScoreList = Array(
      (1,100),
      (2,20),
      (3,66),
      (4,55)
    )
    val stu = sc.parallelize(studentList)
    val cores = sc.parallelize(ScoreList)
    stu.leftOuterJoin(cores).foreach(println)
  }
  def cogroup(): Unit ={
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val studentList = Array(
      (1,"xiaohong"),
      (2,"xiaoli"),
      (3,"xiaohua"),
      (4,"lili")
    )
    val ScoreList = Array(
      (1,100),
      (2,20),
      (3,66),
      (5,25)
    )
    val stu = sc.parallelize(studentList)
    val cores = sc.parallelize(ScoreList)
    val cogroups: RDD[(Int, (Iterable[String], Iterable[Int]))] = stu.cogroup(cores)
    cogroups.foreach(x=>{
      println(x._1)
//      println(x._2._1)
//      println(x._2._2)

    })
  }

  def Mappartitions(): Unit ={
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    // 准备模拟数据
     val arr = Array("张三","李四","王二","麻子")
    // 并行化创建RDD
    val nameRDD: RDD[String] = sc.parallelize(arr,2)
    val hashMap = mutable.HashMap("张三"->278.5,"李四"->290.0,"王二"->301.0,"麻子"->205.0)//并行化集合
    // 类似map, 不同之处在于，map算子，一次就处理一个partition中的一条数据
    // mapPartition算子，一次处理一个partition中所有的数据
    // 推荐使用场景
    // 如果你的RDD的数据量不是很大，那么建议你采用MapPartitions算子替代map算子，可以提高效率
    // 但是如果你的RDD的数据量特别大，比如说10亿条，不建议你使用MapPartitions算子，有可能会内存溢出，造成oom
    //过程就是，写一个Array，然后再写一个可变数组，最后用这个方法，迭代出key，再去取value，因为返回的是一个迭代器，所以要转成迭代器类型，最后循环输出
    val arr2 = nameRDD.mapPartitions(m=>{//怎么看返回值的，用快捷键，然后=>后面的就是返回类型，U是代表我想返回什么，就是什么吧？应该是的吧 Todo

      var list = mutable.ListBuffer[Double]()
      while (m.hasNext){
        val name: String = m.next()
        val nameScore: Option[Double] = hashMap.get(name)
        list ++= nameScore//根据key取value，再将值放入list这个容器中
      }
      list.iterator
    })
    arr2.foreach(println)
  }

  def Distinct: Unit ={
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    // 它对RDD中的数据进行去重
    // 应用场景
    // uv 统计业务
    // uv:user view 每天每个用户可能对网站点击多次
    // 此时，需要对用户进行去重，然后统计出每天有多少用户访问了网站
    // 而不是所有用户访问了网站多少次（pv）
    val arr = Array(
      "user1 2019-02-19 16:11:48",
      "user1 2019-02-19 16:11:49",
      "user1 2019-02-19 16:11:50",
      "user2 2019-02-19 16:11:48",
      "user2 2019-02-19 16:11:48",
      "user2 2019-02-19 16:11:48",
      "user3 2019-02-19 16:11:48",
      "user3 2019-02-19 16:11:48",
      "user3 2019-02-19 16:11:48",
      "user4 2019-02-19 16:11:48",
      "user4 2019-02-19 16:11:48",
      "user4 2019-02-19 16:11:48"
    )
    val user = sc.parallelize(arr,2)
    val uv =user.map(t=>{
      val lines = t.split(" ")
      lines(0)
    }).distinct().count()

    println(uv)
  }

  def Coalesce: Unit = {
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    // 将RDD的partition缩减，减少 也是可以增加
    // 将一定量的数据，压缩到更少的partition中去
    // 建议使用场景，配合filter算子使用
    // 使用filter算子过滤掉很多数据后，比如30%的数据，出现了很多Partition中的数据不均匀情况
    // 此时建议使用coalesce算子 压缩RDD的分区数量
    // 让数据更加紧凑


    // 公司原先有6个部门
    // 不巧 碰到了公司裁员， 裁员以后，有的部门中的人员就没了，现在产生了部门人员分配不均匀状况
    // 此时 公司要做调整，将部门压缩，节省经费
    val arr = List("张三","李四","王五","赵六","田七","猪八","龙久","小倩","小周","小孙")
    // 创建RDD
    val nameRDDs = sc.parallelize(arr,6)
    // 首先查看一下之前的部门分配情况
    val namePartition = nameRDDs.mapPartitionsWithIndex(mapPartitionIndexFunc,true)
//    namePartition.collect.foreach(println)
    // 重新分区
    val namePartition2 = namePartition.coalesce(3)
    // 查看分完区后的数据
    val coaName = namePartition2.mapPartitionsWithIndex(mapPartitionIndexFunc2,true)
    coaName.foreach(println)

  }

  def mapPartitionIndexFunc(i1:Int,iter:Iterator[String]):Iterator[(Int,String)]={
    var list = List[(Int,String)]()
    while (iter.hasNext){
      val name = iter.next()
      list = list.::(i1,name)
    }
    list.iterator
  }

  def mapPartitionIndexFunc2(i1:Int,iter:Iterator[(Int,String)]):Iterator[(Int,(Int,String))]={
   var list = List[(Int,(Int,String))]()
    while (iter.hasNext){
      val ParName: (Int, String) = iter.next()
      list = list.::(i1,ParName)
    }
    list.iterator
  }

  def repartition: Unit ={
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    // 用于增加分区，也可以减少
    // 增加部门
    val arr = List("张三","李四","王五","赵六","田七","猪八","龙久","小倩","小周","小孙")
    // 创建RDD
    val nameRDDs = sc.parallelize(arr,3)
    // 首先查看一下之前的部门分配情况
    val namePartition = nameRDDs.mapPartitionsWithIndex(mapPartitionIndexFunc,true)
    //namePartition.collect.foreach(println)
    // 重新分区
    val namePartition2 = namePartition.repartition(6)
    // 查看分完区后的数据
    val coaName = namePartition2.mapPartitionsWithIndex(mapPartitionIndexFunc2,true)
    coaName.foreach(println)
  }
}
