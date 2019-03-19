package com.day02

/**
  * Trait（接口）
  */
trait TraitDemo {
  def sayHello(name:String)
}

trait MakeTrait{
  def makeTrait(age:Int)
}

class PersonTrait(val name:String) extends TraitDemo with MakeTrait{
  override def sayHello(name: String): Unit = {
    println("hello ," + name)
  }

  override def makeTrait(age: Int): Unit = {
    println("Hi ," + name + " " + age)
  }
}
object TraitTest{
  def main(args: Array[String]): Unit = {
    val personTrait = new PersonTrait("xiaoming")
    println(personTrait.name)
    personTrait.sayHello("xiaohong")
    personTrait.makeTrait(30)
  }
}