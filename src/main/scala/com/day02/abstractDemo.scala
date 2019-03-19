package com.day02

/**
  * 让object继承抽象类
  */
abstract class abstractDemo (var message:String){
  def sayHello(name:String):Unit
}

object HelloImpl extends abstractDemo("hello"){

  override def sayHello(name: String): Unit = {
    println(message + ", " + name)
  }

  def main(args: Array[String]): Unit = {
    HelloImpl.sayHello("小明")
  }
}