package com.day02

/**
  * Trait继承class类
  */
class myClass{
  def MyClassed(msg:String) ={println(msg)}
}

trait MyTrait extends myClass{
  def log(msg:String) =MyClassed(msg)
}

class myClass02 (val name:String) extends MyTrait{
  def getTrait:Unit={
    log(name +"  000")
    MyClassed(name+"  111")
  }
}

object MyTraitTest{
  def main(args: Array[String]): Unit = {
    val leo = new myClass02("Leo")
    leo.getTrait
  }
}