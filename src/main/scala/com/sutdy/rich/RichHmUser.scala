package com.sutdy.rich

import com.sutdy.model.HmUser
/**
 * 隐式转换+比较
 */
class RichHmUser(val t: HmUser) extends Ordered[HmUser] {

  override def compare(that: HmUser): Int = {
    if (t.id > that.id) 1 else -1
  }

}

object RichHmUser {
  def main(args: Array[String]): Unit = {
    println("aaaaaaaaaaaa")
  }
}
