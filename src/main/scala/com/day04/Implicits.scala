package com.day04

/**
  *特殊人群的隐式转换
  */
object Implicits {
  implicit def object2toPerson(obj:Object):SpecialPerson={
    //首先判断是不是学生类型
    if (obj.getClass == classOf[Students]){
      //2.如果是学生类型，就把这个类型放入到特殊群体中
      val students = obj.asInstanceOf[Students]
      new SpecialPerson(students.name)//如果传入的类型等于学生的类型，那个就把这个传入类型强转为Students
      //3.如果不是学生，就有可能是其他年龄的人
    }else if(obj.getClass == classOf[Older]){
      val older = obj.asInstanceOf[Older]
      new SpecialPerson(older.name)
    }else{
      null
    }
  }

}
