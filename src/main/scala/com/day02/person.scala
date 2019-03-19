package com.day02

/**
  * 继承
  */
class person {
  private var name = "leo"
  def getName = name
}

class Stu extends person{
  private var scores = "100"
  def getScore = scores
}

object StuTest{
  def main(args: Array[String]): Unit = {
    val stu = new Stu
    println(stu.getName)
    println(stu.getScore)
  }
}