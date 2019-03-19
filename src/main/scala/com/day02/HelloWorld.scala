package com.day02

/**
  * 入门
  */
class HelloWorld {
  private var name = "zhuzhen"
  def sayHello=name + " " +" HI"
  def getName ={
    println(name)}
}

object HelloWorldTest{
  def main(args: Array[String]): Unit = {

    val helloWorld = new HelloWorld


    println(helloWorld.sayHello)

    helloWorld.getName
  }
}