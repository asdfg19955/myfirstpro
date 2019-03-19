package com.day04

/**
  * 案例:考试签到
  */
 class SignPen {
def write(content:String)=println(content)
 }
class SignPens {
  def getsignPen(name: String)(implicit signPen: SignPen) = {
    signPen.write(name + "...................")

  }
}
  object ImplicitDemo02{
    def main(args: Array[String]): Unit = {
      import com.day04.Implicts.sigPen
      val pens =new SignPens
      pens.getsignPen("zhangsan")
    }
  }

