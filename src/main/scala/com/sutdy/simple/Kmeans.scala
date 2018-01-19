package com.sutdy.simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

/**
 * MLlib机器学习，聚类实例，K-Means算
 *
 * @see https://www.cnblogs.com/shishanyuan/p/4747778.html
 */
class Kmeans {

}

object Kmeans {

  /**
   * 数据格式：
   * 0.0	0.0	0.0
   * 0.1	0.1	0.1
   * 0.2	0.2	0.2
   * ……
   * 9.0	9.0	9.0
   * 9.1	9.1	9.1
   * 9.2	9.2	9.2
   */
  def main(args: Array[String]): Unit = {
    println("aaaaaaaaaaa")
    //    屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //    设置运行环境
    val conf = new SparkConf().setAppName("Kmeans").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //    装载数据集
    val data = sc.textFile("D:/data/spark/mllib-kmeans-data.txt", 1)
    val parsedDate = data.map(s => {
      Vectors.dense(s.split("\t").map(_.toDouble))
    })
    //    将数据集聚类，2个类，20次迭代，进行模型训练形成数据模型
    val numClusters = 2
    val numIterations = 20
    val model = KMeans.train(parsedDate, numClusters, numIterations)
    //打印数据模型的中心点
    println("聚类中心点:")
    for (c <- model.clusterCenters) {
      println("", c.toString())
    }

    //    使用误差平方之和来评估数据模型
    val cost = model.computeCost(parsedDate)
    println("误差平方之和:", cost)

    //   使用模型测试单点数据
    println("Vectors 0.2 0.2 0.2属于集群:", model.predict(Vectors.dense("0.2	0.2	0.2".split("\t").map(_.toDouble))))
    println("Vectors 0.25 0.25 0.25属于集群:", model.predict(Vectors.dense("0.25	0.25	0.25".split("\t").map(_.toDouble))))
    println("Vectors 8 8 8属于集群:", model.predict(Vectors.dense("8	8	8".split("\t").map(_.toDouble))))
    println("Vectors 9 9 9属于集群:", model.predict(Vectors.dense("9	9	9".split("\t").map(_.toDouble))))
    println("Vectors 20 20 20属于集群:", model.predict(Vectors.dense("20	20	20".split("\t").map(_.toDouble))))
    println("Vectors 24.9	24.9	24.9属于集群:", model.predict(Vectors.dense("24.9	24.9	24.9".split("\t").map(_.toDouble))))
    println("Vectors 30 30 30属于集群:", model.predict(Vectors.dense("30	30	30".split("\t").map(_.toDouble))))
    println("Vectors 40属于集群:", model.predict(Vectors.dense("40	40	40".split("\t").map(_.toDouble))))
    println("Vectors 45属于集群:", model.predict(Vectors.dense("45	45	45".split("\t").map(_.toDouble))))
    println("Vectors 46属于集群:", model.predict(Vectors.dense("46	46	46".split("\t").map(_.toDouble))))
    println("Vectors 47属于集群:", model.predict(Vectors.dense("47	47	47".split("\t").map(_.toDouble))))
    println("Vectors 48属于集群:", model.predict(Vectors.dense("48	48	48".split("\t").map(_.toDouble))))
    println("Vectors 49属于集群:", model.predict(Vectors.dense("49	49	49".split("\t").map(_.toDouble))))
    println("Vectors 50属于集群:", model.predict(Vectors.dense("50	50	50".split("\t").map(_.toDouble))))
    println("Vectors 55属于集群:", model.predict(Vectors.dense("55	55	55".split("\t").map(_.toDouble))))
    println("Vectors 66属于集群:", model.predict(Vectors.dense("66	66	66".split("\t").map(_.toDouble))))
    println("Vectors 75属于集群:", model.predict(Vectors.dense("75	75	75".split("\t").map(_.toDouble))))

    //    交叉评估1，只返回结果
    val testdata = data.map(s => {
      Vectors.dense(s.split("\t").map(_.toDouble))
    })
    val long1 = System.currentTimeMillis()
    val result1 = model.predict(testdata)
    result1.saveAsTextFile("D:/data/spark//kmeans-1-rslt-" + long1)
    //    交叉评估2，返回数据集和结果
    data.map(line => {
      val linevectore = Vectors.dense(line.split("\t").map(_.toDouble))
      val prediction = model.predict(linevectore)
      line + "\t" + prediction
    }).saveAsTextFile("D:/data/spark//kmeans-2-rslt-" + long1)

    sc.stop()
  }

}

