package com.day04

class Man (val name :String)

class Superman(val name:String){
  def mans =println("变身！！！超人")
}
object ManTest{
  def main(args: Array[String]): Unit = {
    import com.day04.ChangMan.man2Superman
    val leo =new Man("leo")
    leo.mans
  }
}

