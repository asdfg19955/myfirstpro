package com.day04

object ChangMan {
  implicit def man2Superman(obj:Object):Superman={
    if(obj.getClass == classOf[Man]){
      val man = obj.asInstanceOf[Man]
       new Superman(man.name)
    }else{
      null
    }
  }


}
