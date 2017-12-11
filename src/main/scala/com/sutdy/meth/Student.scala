package com.sutdy.meth
/**
 *
 */
class Student(val name: String) extends Ordered[Student] {
  override def compare(that: Student): Int = {
    if (this.name == that.name)
      1
    else
      -1
  }
}

//将类型参数定义为T<:Ordered[T]
class Pair1[T <: Ordered[T]](val first: T, val second: T) {
  //比较的时候直接使用<符号进行对象间的比较
  def smaller() = {
    if (first < second)
      first
    else
      second
  }
}

object OrderedViewBound {
  val p = new Pair1(Student("摇摆少年梦"), Student("摇摆少年梦2"))
  println(p.smaller)
}

object Student {
  def apply(name: String): Student = {
    new Student(name)
  }
}
