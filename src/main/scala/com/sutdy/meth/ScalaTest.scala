package com.sutdy.meth

import org.apache.spark.SparkContext._

/**
  * scala学习
  */
class ScalaTest {

}

object  ScalaTest {
  def main(args: Array[String]): Unit = {
    println(1 to 10)

    //九九乘法表
    for {
      x <- 1 to 9
      y <- 1 to x

    } {
      println(s"$y*$x = ${x*y}\t")
      if (x==y) println()
    }

    for(i <- 1 until 10) {
      for(j <- 1 to 9) {
        if(j <= i) {
          print(j+"*"+i+"="+i*j + "\t")
        }
        if(j == i) {
          println()
        }
      }
    }
  }
}

object  ScaltaT {
  def main(args: Array[String]): Unit = {
    println(1 until 10)
    println(1 to 10)
  }
}

/*
 * scala中常用特殊符号，学习
 * http://www.mamicode.com/info-detail-2152650.html   scala中常用特殊符号
 */
class Symbol {
//  =>（匿名函数）
  var add = (x:Int) => x+1
//  <- （集合遍历）
//  ++=（字符串拼接）
//  :::三个冒号运算符与::两个冒号运算符
//  -> 构造元组和_N访问元组第N个元素
//  _（下划线）的用法
//  成员变量而非局部变量添加默认值
}

object test2 {
  def main(args: Array[String]): Unit = {
    var symbol = new Symbol
    println(symbol.add(3).toString())

//    -> 构造元组和_N访问元组第N个元素
    val fir = (1,2,3)//定义三元元组

    val one = 1
    val two = 2
    val thr = one -> two
    println(thr)  //构造二元元组
    println(thr._2) //  // 访问二元元组中第二个值

    val obj4 = fir -> thr
    println(obj4)

//    指代集合中的每一个元素
//    例如 遍历集合筛选列表中大于某个值的元素。
    val list1 = List(1,9,0,2,8,3,4,5,6)
    val list11 = list1.filter(_>3)
    println(list11)

//    使用模式匹配可以用来获取元组的组员
    val m = Map(1 -> 2,2->4,23->2,22->3)
    for ((k,_) <- m) println("aa:"+k)

  }
}