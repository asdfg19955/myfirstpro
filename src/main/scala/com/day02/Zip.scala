package com.day02

/**
  * idea中的拉链
  */
object Zip {
  def main(args: Array[String]): Unit = {
    val names = Array("Leo","Jack","Jen")
    val Scores = Array(80,90,70)
     val tuples = names.zip(Scores)
    for((names,scores)<- tuples){
      println(names+"  "+scores)
    }
    tuples.toMap
  }
}
