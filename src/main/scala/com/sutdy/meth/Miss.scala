package com.sutdy.meth

import com.sutdy.model.HmUser
import ZtfPreDef._

/**
 * 柯里化+隐式转换+比较
 */
class Miss[T] {

  def choose(x: T, y: T)(implicit f: T => Ordered[T]): T = {
    if (x > y) x
    else y
  }
}

object Miss {

  def main(args: Array[String]): Unit = {
    var user1 = HmUser(1, "aaa", "zhaotf")
    var user2 = HmUser(2, "abc", "ztf")
    var user3 = HmUser(3, "bbb", "hmlc")
    val mi = new Miss[HmUser]
    val flg1 = mi.choose(user1, user2).id
    val flg2 = mi.choose(user2, user3).id

    println("柯里化+隐式转换+比较:", flg1, flg2)
  }

}
