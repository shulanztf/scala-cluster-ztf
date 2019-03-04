package com.sutdy.spark.examples.scala.generics

/**
  * https://bit1129.iteye.com/blog/2186577 ,【Scala九】Scala核心三：泛型
  */
class GenericClass[K,V](val k: K,val v: V) {
  def print(): Unit = {
    println(k+"--"+v)
  }

}

object GenericClass {
  def main(args: Array[String]): Unit = {
    val gc2 = new GenericClass("ERT",200)
    val gc = new GenericClass[String,Integer]("ABC",100)
    gc2.print()
    gc.print()
  }
}


/**
  * https://blog.csdn.net/lovehuangjiaju/article/details/47360869,Scala入门到精通——第二十一节 类型参数（三）-协变与逆变
  * 协变
  * @param head
  * @param tail
  * @tparam T
  */
class List[+T](val head:T,val tail:List[T]) {
  def prepend[U>:T](newHead:U): List[U] =new List(newHead,this)

  override def toString() = ""+head
}
object NonVariance {
  def main(args: Array[String]): Unit = {
    val list:List[Any]  = new List[String]("aaaa",null)
    println(list)
    val list1:List[Any]=new List[Integer](33,null)
    println(list1)
  }
}

//class Animal{}
//class Bird extends Animal {}
//class Consumer[-S,+T]() {
//  def m1[U >: T](u: U):T = {new T} //协变，下界
//  def m2[U <: S](s: S):U = {new U} //逆变，上界
//}
//
//object Consumer {
//  def main(args: Array[String]): Unit = {
//    val c:Consumer[Animal,Bird] = new Consumer[Animal,Bird]()
//    val c2:Consumer[Bird,Animal] = c
//    c2.m1(new Animal)
//    c2.m2(new Bird)
//  }
//}
