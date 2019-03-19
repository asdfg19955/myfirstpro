package com.day02

/**
  * 抽象变量
  */
abstract class AbsVal {
  val name:String
}

class AbsVar extends AbsVal{
   val name: String = "Jack"
}

object AbsValTest{
  def main(args: Array[String]): Unit = {
    val absVar = new AbsVar
    // 抽象类不能被实例化
    println(absVar.name)
  }
}
