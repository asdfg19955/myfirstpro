package com.day02

/**
  * 函数式编程
  */
object Function {
  def main(args: Array[String]): Unit = {
    val arr = Array(1,2,3,4,5)
    arr.sortWith(_ < _)
    List("Hello A").flatMap(_.split(" "))
  }
}
