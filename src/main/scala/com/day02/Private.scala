package com.day02

/**
  * private修饰的权限
  */
class Private {
  // 用这种声明方式表示最严格的声明方式，伴生对象也无法访问到
  private[this] val hello = "张三"
  // 这种声明方式，伴生对象可以访问到
  private var name = "老鼠爱大米"
  // 谁都可以访问
  val names = ""
}

object PrivateTest{
  def main(args: Array[String]): Unit = {

    val privateDemo = new Private

    privateDemo.names
  }
}


  object Private{
    def main(args: Array[String]): Unit = {
      val pris= new Private
      println(pris.name)
    }
  }