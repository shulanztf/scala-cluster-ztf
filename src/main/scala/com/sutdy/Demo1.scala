package com.sutdy

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Mapper

/**
  * https://blog.csdn.net/hopeatme/article/details/52655576 Scala 开发简单mapreduce 程序
  */
class Demo1 extends {

}

object HelloWorld {
  def main(args: Array[String]):Unit = {
    println("Hello, world!")
  }
}

/**
  * https://blog.csdn.net/sinat_35045195/article/details/78851476
  * case关键字在scala的作用
  */
object fun2 {
  def main(args: Array[String]): Unit = {
    val model:Seq[(String,Int)] = ("aa"->19)::("bb"->22)::("cc"->12)::("dd"->33)::Nil
    model foreach{
      case (name,age) => println(name +"--"+age)
      case _ => println("eeeeeeeeeeee")
    }
  }
}

/**
  * https://blog.csdn.net/sinat_35045195/article/details/78851476
  * case关键字在scala的作用
  */

//import scala.language.postfixOps
case class  ForFun(name:String)
object fun3 {
  def main(args: Array[String]): Unit = {
    val ff = ForFun.apply("aaa")
    println(ff.name)

    val ff2 = new ForFun("ccc")
    println(ff2.name+"--"+ff2.hashCode()+"--"+ff2.toString)
    println(ff.equals(ff2))
  }
}


























