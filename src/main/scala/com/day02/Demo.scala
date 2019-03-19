package com.day02

/**
  * 一般的话在object中都进行初始静态变量或者方法
  */
object Demo {
  private var Num = 2
  println("this Demo Object!!")
  def getNum =Num
}

object DemoTest{
  def main(args: Array[String]): Unit = {
    println(Demo.getNum)
  }
}