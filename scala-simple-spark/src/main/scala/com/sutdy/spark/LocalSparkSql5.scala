package com.sutdy.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

import scala.util.Random


/**
  *
  */
class LocalSparkSql5 {

}

object LocalSparkSql5 {

  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val conf = SparkSession.builder().appName("").master("").getOrCreate().sparkContext
    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.start()
    ssc.awaitTermination()
  }

  val conf = SparkSession.builder().appName("").master("").getOrCreate().sparkContext

  /**
    * 2.3 数据倾斜解决方案-2.3.3 局部聚合和全局聚合
    * 思路：这个方案的核心实现思路就是进行两阶段聚合。第一次是局部聚合，先给每个key都打上一个随机数，比如10以内的随机数，此时原先一样的key就变成不一样的了，
    * 比如(hello, 1) (hello, 1) (hello, 1) (hello, 1)，就会变成(1_hello, 1) (1_hello, 1) (2_hello, 1) (2_hello, 1)。接着对打上随机数后的数据，
    * 执行reduceByKey等聚合操作，进行局部聚合，那么局部聚合结果，就会变成了(1_hello, 2) (2_hello, 2)。然后将各个key的前缀给去掉，就会变成(hello,2)(hello,2)，
    * 再次进行全局聚合操作，就可以得到最终结果了，比如(hello, 4)。
    * https://www.cnblogs.com/arachis/p/Spark_Shuffle.html
    */
  def solution1(): RDD[(String, Int)] = {
    val pairs = conf.textFile("/home/*.txt").flatMap(line => {
      line.split(",")
    })

    val SPLIT = "-"
    val prefix = new Random().nextInt(10)
    pairs.map(lines => {
      (prefix + SPLIT + lines, 1)
    }).reduceByKey((v1, v2) => {
      v1 + v2
    }).map(t2 => {
      (t2._1.split(SPLIT)(1), t2._2)
    }).reduceByKey((v1, v2) => v1 + v2)
  }

  /**
    * 2.3.4 将reduce join转为map join（（小表几百M或者一两G））
    * 注意，以上操作，建议仅仅在rdd2的数据量比较少（比如几百M，或者一两G）的情况下使用。因为每个Executor的内存中，都会驻留一份rdd2的全量数据。
    * 思路：不使用join算子进行连接操作，而使用Broadcast变量与map类算子实现join操作，进而完全规避掉shuffle类的操作，彻底避免数据倾斜的发生和出现。
    * 将较小RDD中的数据直接通过collect算子拉取到Driver端的内存中来，然后对其创建一个Broadcast变量；接着对另外一个RDD执行map类算子，
    * 在算子函数内，从Broadcast变量中获取较小RDD的全量数据，与当前RDD的每一条数据按照连接key进行比对，如果连接key相同的话，
    * 那么就将两个RDD的数据用你需要的方式连接起来。 　
    * https://www.cnblogs.com/arachis/p/Spark_Shuffle.html
    */
  def solution2(): RDD[(String)] = {
    val vid_pairs = conf.textFile("").flatMap(x => {
      x.split("")
    }).map(word => {
      (word, 1)
    })

    val broadcast = conf.broadcast(conf.textFile("/home/*.txt").map(line => {
      val SPLIT = ","
      val ss = line.split(SPLIT)
      (ss(1), ss(2))
    }).collectAsMap())
    //    inner join
    vid_pairs.filter(line => {
      broadcast.value.keySet.contains(line._1)
    }).map(t2 => {
      broadcast.value.get(t2._1).get
    })

  }

  /**
    * 2.3.5 采样倾斜key并分拆join操作（join的两表都很大，但仅一个RDD的几个key的数据量过大）
    * 思路：1)对包含少数几个数据量过大的key的那个RDD，通过sample算子采样出一份样本来，然后统计一下每个key的数量，计算出来数据量最大的是哪几个key。
    * 2)然后将这几个key对应的数据从原来的RDD中拆分出来，形成一个单独的RDD，并给每个key都打上n以内的随机数作为前缀，而不会导致倾斜的大部分key形成另外一个RDD。
    * 3)接着将需要join的另一个RDD，也过滤出来那几个倾斜key对应的数据并形成一个单独的RDD，将每条数据膨胀成n条数据，这n条数据都按顺序附加一个0~n的前缀，不会导致倾斜的大部分key也形成另外一个RDD。
    * 4)再将附加了随机前缀的独立RDD与另一个膨胀n倍的独立RDD进行join，此时就可以将原先相同的key打散成n份，分散到多个task中去进行join了。
    * 5)而另外两个普通的RDD就照常join即可。
    * 6)最后将两次join的结果使用union算子合并起来即可，就是最终的join结果。
    */
  def solution3(): RDD[(String, (Int, String))] = {
    val pairs1 = conf.textFile("/home/*.txt").flatMap(x => x.split(",")).map(x => (x, 1))
    val pairs2 = conf.textFile("/home/*.txt").flatMap(x => x.split(",")).map(x => (x, 1))
    val skewedKey = pairs1.sample(false, 0.1).reduceByKey((v1, v2) => v1 + v2).map(t2 => t2.swap).sortByKey(false).first()._2
    val skewedRDD = pairs1.filter(x => {
      x._1.equals(skewedKey)
    })
    val conmmonRDD = pairs1.filter(x => {
      !x._1.equals(skewedKey)
    })
    val SPLIT = "_"
    val size = 100
    val skewedRDD2 = pairs2.filter(x => {
      x._1.equals(skewedKey)
    })
    val joinedRDD1 = skewedRDD.map(t2 => {
      val prefix = new Random().nextInt(size)
      (prefix + SPLIT + t2._1, t2._2)
    }).join(skewedRDD2).map(t2 => {
      val key = t2._1.split(SPLIT)(1)
      (key, t2._2)
    })

    val joineRDD2 = conmmonRDD.join(pairs2)
    //    适应作者代码，而特殊处理
    joinedRDD1.union(joineRDD2).map(x => {
      (x._1, (x._2._1, x._1))
    })
  }

  /**
    *  2.3.6 使用随机前缀和扩容RDD进行join(RDD中有大量的key导致数据倾斜)
    * 思路：将含有较多倾斜key的RDD扩大多倍，与相对分布均匀的RDD配一个随机数。
    */
  def solution4(): RDD[(String, (String, String))] = {
    val pairs1 = conf.textFile("/home/*.txt").flatMap(x => x.split(",")).map(x => (x, 1))
    val pairs2 = conf.textFile("/home/*.txt").flatMap(x => x.split(",")).map(x => (x, 1))
    val SPLIT = "_"
    val size = 100
    val skewedRDD = pairs1.flatMap(t2 => {
      val arrar = new Array[Tuple2[String, String]](size)
      for (i <- 0 to size) {
        arrar(i) = (new Tuple2[String, String](i + SPLIT + t2._1, t2._2.toString))
      }
      arrar
    })

    pairs2.map(t2 => {
      val prefix = new Random().nextInt(size)
      (prefix + SPLIT + t2._1, t2._2)
    }).join(skewedRDD).map(t2 => {
      val key = t2._1.split(SPLIT)(1)
      //    适应作者代码，而特殊处理
      (key, (t2._2._1.toString, ""))
    })

  }

}

