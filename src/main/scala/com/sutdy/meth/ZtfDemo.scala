package com.sutdy.meth

object ZtfDemo {
  def main(args: Array[String]) {
    println("abced")
  }

  def factorial(x: BigInt): BigInt = {
    if (x <= 1) {
      return 1
    }
    x * factorial(x - 1)
  }

}
