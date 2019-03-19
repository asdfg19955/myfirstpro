package com.day02

/**
  * Getter 和 Setter方法
  */
class Student {
  val name = "Jack"
}

object StudentTest{
  def main(args: Array[String]): Unit = {
    val jack = new Student
    // 如果用 val 声明的变量，那么它只有getter方法，那么var声明的getter和setter都有

    println(jack.name)
  }
}