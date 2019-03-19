package com.myproject

import com.day02.InstanceOf02

class Instance

class Instanceof extends Instance{
  def main(args: Array[String]): Unit = {
    val of : Instance=new Instanceof//这应该算是多态

    //2.创建一个子类的空类
    var of02:Instanceof=null

    //3.接下来是要进行类型转换
    if(of.isInstanceOf[Instanceof]){
      of02=of.asInstanceOf[Instanceof]
    }

    }
  }

