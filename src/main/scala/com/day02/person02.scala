package com.day02

/**
  * override 和 super
  */
class person02 {
  private var name = "zhangsan"
  def getName = name
  val age = 0

}

class Stu02 extends person02{
  private var score = 100

  // 利用override 重写父类的方法,通过super 来调用父类的getName方法
  override def getName: String = score + super.getName

  override val age: Int = 30

}

object Stu02Test{
  def main(args: Array[String]): Unit = {
    val person = new person02
    println(person.age)

    val stu = new Stu02
    println(stu.getName)
    println(stu.age)
  }
}