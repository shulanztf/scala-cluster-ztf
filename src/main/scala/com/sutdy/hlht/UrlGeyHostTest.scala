package com.sutdy.hlht

import java.util.regex.Pattern

import org.apache.commons.lang3.StringUtils

/**
  * @author 赵腾飞
  * @date 2019/10/28/028 16:42
  * https://blog.csdn.net/demon_LL/article/details/78364143 scala正则表达式获取url的host
  */
class UrlGeyHostTest {

}
//2019-10-28
//14:04:14,434
//INFO
//[qtp1382756158-39784]
//UHomeCompositeTokenGranter
//[UHomeCompositeTokenGranter.java:59]
//-
//[UOCLOGPRINT]
//haokongqi
//POST
///oauth/token/client_credentials
//20191028140414
//success
object TomcatLogTest {
//  val text = "2019-10-28 14:04:14,434 INFO [qtp1382756158-39784] UHomeCompositeTokenGranter [UHomeCompositeTokenGranter.java:59] - [UOCLOGPRINT] haokongqi POST /oauth/token/client_credentials 20191028140414 success"
  val text = "2019-10-28 14:04:14,548 INFO [qtp1382756158-105159] LogPrintFormatAspect [LogPrintFormatAspect.java:59] - [UOCLOGPRINT] uplusappserver POST https://account-api.haier.net/v2/qr-login/uhome/poll 20191028140414 success"
  def main(args: Array[String]): Unit = {
    var arr = StringUtils.split(text," ")
//    var word = null
//    for (word <- arr) {
//      println(word)
//    }
    var time = arr(0) + " " + arr(1)
    println(time)
    println(arr(8))
    println(arr(10))

var    map = getIp(arr(10))
    println(map.nonEmpty)
    println(map)

  }

  def getIp(url:String): Map[String,String] = {
    var map:Map[String,String] = Map()
    try {
      var url1 = new java.net.URL(url)
//      println(url.getProtocol)
//      println(url.getAuthority)
//      println(url.getPath)
      map += ("protocol" -> url1.getProtocol)
      map += ("authority" -> url1.getAuthority)
      map += ("path" -> url1.getPath)
    }catch  {
      case ex:Exception => {
        //        println("")
        ex.printStackTrace()
//        println(url)
      }
    }
//    Map("protocol"->"32","authority"->"fsewf","path"->"zzzzzzzzzzzz")
    map
  }
}

object UrlGeyHostTest {


  def main(args: Array[String]): Unit = {
//    var url1 = "https://account-api.haier.net/v2/get-qrcode"
//    var url1 = "https://127.0.0.1:8080/v2/get-qrcode"
    var url1 = "/oauth/token/client_credentials"
    try {
      var url = new java.net.URL(url1)
      println(url.getProtocol)
      println(url.getAuthority)
      //    println(url.getHost)
      println(url.getPath)
    }catch  {
      case ex:Exception => {
//        println("")
        ex.printStackTrace()
        println(url1)
      }
    }



  }

//  def main(args: Array[String]): Unit = {
//    //传参
////    val url1 = "http://tieba.baidu.com/p/4336698825"
//    val url2 = "https://mp.weixin.qq.com/s?__biz=MzIyODgyNDk0OQ==&mid=2247483988&idx=3&sn=7181bbef257e27014051272d785eeafd&scene=4#wechat_redirect"
//    var host = ""
//    val p = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+")
//    val matcher = p.matcher(url2)
//    if (matcher.find()) {
//      host = matcher.group()
//      println(matcher.group(0))
//      println(matcher.group(1))
//      println(matcher.group(2))
//
//    }
//    println(host)
//  }
}
