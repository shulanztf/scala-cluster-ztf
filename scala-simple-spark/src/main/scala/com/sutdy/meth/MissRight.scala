package com.sutdy.meth

import com.sutdy.model.HmUser
import ZtfPreDef._

/**
 * XN学堂，自定义类型隐匿转换
 */
class MissRight[T <% Ordered[T]](val first: T, val second: T) {

  def choose(first: T, second: T): T = {
    if (first >= second) first else second
  }

}

object MissRight {

  def main(args: Array[String]): Unit = {
    var user1 = HmUser(1, "aaa", "zhaotf")
    var user2 = HmUser(2, "abc", "ztf")
    var user3 = HmUser(3, "bbb", "hmlc")
    val flg = user1.compare(user2)
    val flg1 = user3.compare(user2)
    println("隐式转换", flg, flg1)
    var um = new MissRight(user1, user2)
    println("隐式转换aa", um.choose(user1, user3).id)
  }

}
