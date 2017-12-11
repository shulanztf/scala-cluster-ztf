package com.sutdy.meth

import scala.runtime.RichInt

/**
 *
 */
class ZtfDemo {
  //  implicit val abc: Int = 100

  /**
   * 柯里化
   */
  def sum(x: Int)(y: Int): Int = {
    x + y
  }

  /**
   * 柯里化,隐式转换,implicit
   */
  def sum1(x: Int)(implicit y: Int = 5): Int = {
    x * y
  }

}

object ZtfDemo {
  val i1: Int = 1
  def main(args: Array[String]) {
    val demo = new ZtfDemo
    //    println("柯里化:", demo.sum(3)(9))
    //    		println("柯里化:", demo.sum(3)_)
    //    val m1 = demo.sum(5)_
    //    println("柯里化:", m1(9))
    //    println("柯里化,隐s式转换,implicit", demo.sum1(6))
    //    println("柯里化,隐式转换,implicit", demo.sum1(6)(6))
    //    println("柯里化,隐式转换,implicit", m2(3))
    println("柯里化,隐式转换,implicit", m2(3))
    println("柯里化,隐式转换,implicit", m2(3)(5))

    //    println("abc:", i1.to(10))
    //    println("abc:", 1.to(10))
  }

  implicit def toInt(x: Int) = {
    new runtime.RichInt(x)
  }

  //  //  隐式值,必须为静态
  //  implicit val iml: Int = 1000

  def m2(x: Int)(implicit y: Int = 20): Int = x + y

  //  def m3(x: Int)(implicit y: Int = 100): Int = x + y

  def factorial(x: BigInt): BigInt = {
    if (x <= 1) {
      return 1
    }
    x * factorial(x - 1)
  }

}
