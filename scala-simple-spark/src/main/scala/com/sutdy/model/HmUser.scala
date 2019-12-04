package com.sutdy.model
/**
 *
 */
class HmUser(val id: Long, val code: String, val name: String) {
}

object HmUser {
  //  def main(args: Array[String]): Unit = {
  //    //    var user = new HmUser
  //    //    var user = HmUser("aaa")
  //    var user = HmUser(1, "aeofw", "zhaotf")
  //    println("aaaa", user.name, user.code, user.id)
  //  }
  //
  //  def apply(uname: String): HmUser = {
  //    var user = new HmUser
  //    user.name = uname
  //    user
  //  }
  def apply(id: Long, code: String, uname: String): HmUser = {
    new HmUser(id, code, uname)
  }

}
