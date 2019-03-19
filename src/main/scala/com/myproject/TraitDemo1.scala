package com.myproject


trait TraitDemo1 {
  def playGame(game:String)
}
trait Player{
  def playerName(name:String)
}
class  Person(val game:String) extends TraitDemo1 with Player{


  override def playGame(game: String): Unit = {
    println(game+"好好玩")
  }
  override def playerName(name: String): Unit ={
    println("hi"+name+""+ game)
  }

}
object Person{
  def main(args: Array[String]): Unit = {
    val person = new Person("dnf")
    person.playGame("lol")
    person.playerName("xiaoming")
  }
}
