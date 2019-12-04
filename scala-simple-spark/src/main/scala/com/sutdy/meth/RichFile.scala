package com.sutdy.meth

import java.io.File
//import scalaz.effect.Resource
import scala.io.Source
import ZtfPreDef._

/**
 * 隐式转换
 */
class RichFile(val f: File) {
  val filePath = "/data/zxb-wx-msg.txt"
  def read(): String = {
    //    Source.fromFile(filePath).mkString
    Source.fromFile(f).mkString
  }

}

object RichFiel {
  val filePath = "/data/zxb-wx-msg.txt"

  def main(args: Array[String]): Unit = {
    //    val f1 = new File("/data/zxb-wx-msg.txt");
    //    val str = new RichFile(new File(filePath)).read()

    val str = new File(filePath).read

    println("隐式转换", str)
  }

}

