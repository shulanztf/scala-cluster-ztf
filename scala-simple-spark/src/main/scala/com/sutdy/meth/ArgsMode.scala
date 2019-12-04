package com.sutdy.meth

object ArgsMode {

  def main(args: Array[String]) {
    val rs = sumA(3, 5, 12, 3, 5, 32);
    println("ztf:", rs)
  }

  def sumA(args: Int*): Int = {
    var rs = 0
    for (i <- args) {
      rs += i
    }
    rs
  }

}
