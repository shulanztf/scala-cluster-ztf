package com.sutdy.mr

import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description:Spark RDD持久化
  * http://blog.51cto.com/xpleaf/2108614 Spark笔记整理（五）：Spark RDD持久化、广播变量和累加器
  * Auther: zhaotf
  * Date: 2018/11/15 0015 13:32
  */
object _01SparkPersistOps {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(_01SparkPersistOps.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    val logger = Logger.getLogger("org.apache.spark")

    var start = System.currentTimeMillis()
    val linesRDD = sc.textFile("/data/spark/source/word-count-2.txt")

    //    执行第一次RDD的计算
    val retRDD = linesRDD.filter(StringUtils.isNotBlank(_)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //    retRDD.count()
    retRDD.collect().foreach(println("第一次", _))
    println("第一次计算消耗的时间：" + (System.currentTimeMillis() - start) + "ms")

    //  执行第二次RDD的计算
    start = System.currentTimeMillis()
    //    retRDD.collect().foreach(println("第二次",_))
    //    retRDD.collect().foreach(f => {
    //      logger.info("第二次" + f)
    //    })
    retRDD.saveAsTextFile("/data/spark/source/word-count-2-rslt")
    println("第二次计算消耗的时间：" + (System.currentTimeMillis() - start) + "ms")

    // 持久化使用结束之后，要想卸载数据
    //    linesRDD.unpersist()

    sc.stop()

  }

}

/**
  * 共享变量
  * 我们在dirver中声明的这些局部变量或者成员变量，可以直接在transformation中使用，
  * 但是经过transformation操作之后，是不会将最终的结果重新赋值给dirver中的对应的变量。
  * 因为通过action，触发了transformation的操作，transformation的操作，都是通过
  * DAGScheduler将代码打包 序列化 交由TaskScheduler传送到各个Worker节点中的Executor去执行，
  * 在transformation中执行的这些变量，是自己节点上的变量，不是dirver上最初的变量，我们只不过是将
  * driver上的对应的变量拷贝了一份而已。
  *
  *
  * 这个案例也反映出，我们需要有一些操作对应的变量，在driver和executor上面共享
  *
  * spark给我们提供了两种解决方案——两种共享变量
  * 广播变量
  * 累加器
  */
object _02SparkShareVariableOps {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(_01SparkPersistOps.getClass.getSimpleName())
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

    val linesRDD = sc.textFile("/data/spark/source/word-count-1.txt")
    val wordsRDD = linesRDD.filter(StringUtils.isNotBlank(_)).flatMap(_.split(" "))
    var num = 0 // 共享变量:广播变量?累加器?
    val parisRDD = wordsRDD.map(word => {
      num += 1
      println("map--->num = ", word, num)
      (word, 1)
    })
    val retRDD = parisRDD.reduceByKey(_ + _).sortBy(_._2, false)

    println("num = ", num)
    //    retRDD.foreach(println)
    retRDD.saveAsTextFile("/data/spark/source/word-count-1-rslt")
    println("num = ", num)

    sc.stop()
  }
}

/**
  * 使用Spark广播变量
  *
  * 需求：
  * 用户表：
  * id name age gender(0|1)
  *
  * 要求，输出用户信息，gender必须为男或者女，不能为0,1
  */
object _03SparkBroadcastOps {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(_03SparkBroadcastOps.getClass.getSimpleName())
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

    val userList = List("001,刘向前,18,0",
      "002,冯  剑,28,1",
      "003,李志杰,38,0",
      "004,郭  鹏,48,2")

    val genderMap = Map("0" -> "女", "1" -> "男") // 广播变量
    val genderMapBC: Broadcast[Map[String, String]] = sc.broadcast(genderMap)

    val userRDD = sc.parallelize(userList)
    val retRDD = userRDD.map(info => {
      val prefix = info.substring(0, info.lastIndexOf(","))
      val gender = info.substring(info.lastIndexOf(",") + 1)
      val genderMapValue = genderMapBC.value
      val newGender = genderMapValue.getOrElse(gender, "男")
      prefix + "," + newGender
    })

    retRDD.foreach(println)
    sc.stop()

  }
}


/**
  * Spark共享变量之累加器Accumulator
  *
  * 需要注意的是，累加器的执行必须需要Action触发
  */
object _04SparkAccumulatorOps {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(_04SparkAccumulatorOps.getClass.getSimpleName())
    val sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

    // 要对这些变量都*7，同时统计能够被3整除的数字的个数
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)

    val listRDD: RDD[Int] = sc.parallelize(list)
    var counter = 0
    val counterACC = sc.accumulator[Int](0) // 累加器
    val mapRDD = listRDD.map(num => {
      counter += 1
      if (num % 3 == 0) {
        counterACC.add(1)
      }
      //      println("aa", counterACC.value) // TODO，task中不能读取累加器的值，否则，Caused by: java.lang.UnsupportedOperationException: Can't read accumulator value in task
      num * 7
    })
    // 下面这种操作又执行了一次RDD计算，所以可以考虑上面的方案，减少一次RDD的计算
    // val ret = mapRDD.filter(num => num % 3 == 0).count()
    mapRDD.foreach(println)
    println("counter===", counter)
    println("counterACC===", counterACC.value)
    sc.stop()
  }
}



































