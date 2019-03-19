package com.day02

/**
  * Trait中定义具体方法
  */
trait Logger {
  def log (mes:String) = mes
  val num :Int
}

class Logs(val name:String) extends Logger{
  def makeTrait(age:Int):Unit={
    println(log(name) + "进行打印log。。。" + age)
  }

   val num: Int = 23
}

object LoggerTest{
  def main(args: Array[String]): Unit = {
    val lwd = new Logs("lwd")
    lwd.makeTrait(23)
  }
}