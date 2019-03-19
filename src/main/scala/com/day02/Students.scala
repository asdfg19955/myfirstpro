package com.day02

/**
  * 构造器--辅助构造器
  */
class Students {
  var names = "leo"
  var age = 0
  def this(names:String){
    this()
    println(names)
  }
}

object StudentsDemo{
  def main(args: Array[String]): Unit = {
    new Students()
  }
}