package com.day02

/**
  * 类型转换
  */
class InstanceOf

class InstanceOf02 extends InstanceOf

object InstanceOfTest{
  def main(args: Array[String]): Unit = {
    // 子类返回父类的类型
    val of: InstanceOf = new InstanceOf02

    // 先创建一个子类的空类
    var of02 :InstanceOf02 =null

    // 接下来进行转换，首先转换前要进行类型的判断
    if(of.isInstanceOf[InstanceOf02]){

      of02 = of.asInstanceOf[InstanceOf02]
    }

  }
}
