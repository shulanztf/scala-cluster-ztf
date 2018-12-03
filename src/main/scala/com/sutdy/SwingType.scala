package com.sutdy

/**
  * Description: Scala的隐式转换
  * Auther: zhaotf
  * Date: 2018/11/26 0026 09:25
  * https://www.cnblogs.com/MOBIN/p/5351900.html   深入理解Scala的隐式转换
  */
class SwingType {
  def wantLearned(sw: String): Unit = {
    println("兔子已经学会了" + sw)
  }
}

object swimming {
  implicit def learningType(s: AmialType): SwingType = {
    new SwingType
  }
}

class AmialType
object AmialType extends App {
  implicit com.sutdy.swimming._
  val rabbit = new AmialType
  rabbit.wantLearned("cccc")

}


//case class AmialType