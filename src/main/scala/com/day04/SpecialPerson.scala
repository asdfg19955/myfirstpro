package com.day04

/**
  * 案例：特殊售票窗口（只接受特殊人群，比如学生，老人等）
  */
class SpecialPerson(val name : String)

class Students(val name:String)

class Older(val name:String)

class Tested{
  var ticketNumber = 0
  def SpcialTicket(p:SpecialPerson)={
    ticketNumber +=1
  }
}
object ImplictitDemo{
  def main(args: Array[String]): Unit = {
    //这里调用了隐式转换，但是需要我们自己导包
    //那么这个隐式转换是怎么查找的呢？
    //首先谁去内部，去寻找隐式转换的方法，如果找不到，那么我们就要自己导包，它就会我们写好的程序执行
    //调用隐式方法或者隐式类
    import com.day04.Implicits.object2toPerson
    val xiaoming = new Students("小明")
    val wangdaye = new Older("王大爷")
    val tested = new Tested
    tested.SpcialTicket(xiaoming)

  }
}

