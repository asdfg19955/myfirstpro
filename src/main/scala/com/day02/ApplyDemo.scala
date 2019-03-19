package com.day02

/**
  * 初始化方法 apply
  * @param name
  */
class ApplyDemo (val name:String){
}

object ApplyDemo{
  def apply(name:String) = new ApplyDemo(name)
}

object applyTest{
  def main(args: Array[String]): Unit = {
    val applyDemo = ApplyDemo("zhangsan")
    println(applyDemo.name)
  }
}