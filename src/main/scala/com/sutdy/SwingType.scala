package com.sutdy

/**
  * Description: Scala的隐式转换
  * Auther: zhaotf
  * Date: 2018/11/26 0026 09:25
  * https://www.cnblogs.com/MOBIN/p/5351900.html   深入理解Scala的隐式转换
  */
class SwingType {
  def wantLearned(sw: String): Unit = {
    println("兔子已经学会了" + sw)
  }
}

/**
  * 隐式转换调用类中本不存在的方法
  * 通过隐式转换，使对象能调用类中本不存在的方法
  */
object swimming {
  implicit def learningType(s: AmialType): SwingType = {
    new SwingType
  }
}

class AmialType

object AmialType extends App {

  import com.sutdy.swimming._

  val rabbit = new AmialType
  rabbit.wantLearned("cccc")
}

object test3 {
  def main(args: Array[String]): Unit = {
    import com.sutdy.swimming._
    val at = new AmialType
    swimming.learningType(at).wantLearned("ooo")
    at.wantLearned("ttt")
    // 隐式调用了  swimming.learningType(at).wantLearned("ooo")
    val lt = swimming.learningType(at)
    lt.wantLearned("xxx")
  }
}


//case class AmialType
/**
  * 隐式值
  */
object test {

  def persion(implicit name: String): String = {
    name
  }

  def main(args: Array[String]): Unit = {
    println(persion("zhaotf"))

    implicit val p = "mobin"
    //    implicit val p1 = "ccc"

    println(persion)
  }
}


/**
  * 隐式视图,隐式转换为目标类型：把一种类型自动转换到另一种类型
  * 将整数转换成字符串类型：
  */
object test1 {

  def foo(msg: String): Unit = {
    println(msg)
  }

  def main(args: Array[String]): Unit = {
    foo("hmlc")

    implicit def intToString(x: Int) = x.toString

    foo(33)
  }
}


/**
  * 隐式类：
  * 在scala2.10后提供了隐式类，可以使用implicit声明类，但是需要注意以下几点：
  *1.其所带的构造参数有且只能有一个
  *2.隐式类必须被定义在类，伴生对象和包对象里
  *3.隐式类不能是case class（case class在定义会自动生成伴生对象与2矛盾）
  *4.作用域内不能有与之相同名称的标示符
  */
object Stringutils {

  implicit class StringImport(val s: String) {
    def increment(): String = s.map((x: Char) => {
      println("a", x)
      (x + 1).toChar
    })
  }

}

object Main extends App {

  import com.sutdy.Stringutils._

  println("zhaotf".increment)
}

object test4 {
  def main(args: Array[String]): Unit = {
    import com.sutdy.Stringutils._
    println("abc".increment)

    val o1 = new StringImport("abc")
    println(o1.increment)

  }
}






























