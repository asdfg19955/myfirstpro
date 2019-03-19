package com.myproject

import org.apache.spark.{SparkConf, SparkContext}

object Tup {
  def main(args: Array[String]): Unit = {
    joinRdd()

  }
  def joinRdd() {
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val name= Array((1,"spark"),(2,"flink"),(3,"hadoop"),(4,"java"))
    val score= Array((1,100),(2,90),(3,80),(5,90))
    val namerdd=sc.parallelize(name)
    val scorerdd=sc.parallelize(score)
    val result = namerdd.join(scorerdd)
    result.collect.foreach(println)
  }

}
