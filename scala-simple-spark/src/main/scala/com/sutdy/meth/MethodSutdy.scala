package com.sutdy.meth

object MethodSutdy {

  def main(args: Array[String]) {
    println("abcde", eachTest(12, 23, 33))
    println("ztf", eachTest1(1, 3, 6))
  }

  //  def sum(x: Int*): Int = {
  //    val arr = x
  //    var rslt = 0
  //    rslt
  //  }
  /**
   * 可变参数
   */
  def eachTest(x: Int*): Int = {
    var rslt = 0
    for (i <- x) {
      rslt += i
    }
    rslt
  }

  def eachTest1(xC: Int*): Int = {
    var rs = 0
    for (i <- xC) rs += i
    rs
  }

}