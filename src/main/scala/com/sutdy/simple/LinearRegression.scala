package com.sutdy.simple

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

/**
 * 回归算法实例,线性回归（Linear Regression）
 * @see https://www.cnblogs.com/shishanyuan/p/4747778.html
 */
class LinearRegression {

}

object LinearRegression {

  def main(args: Array[String]): Unit = {
    //    屏蔽不必要的日志显示终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //    设置运行环境
    val conf = new SparkConf().setAppName("Kmeans").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //    装载数据集
    val data = sc.textFile("")
    val parsedData = data.map(line => {
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split("\t").map(_.toDouble)))
    })
    //  构建模型
    val numIterations = 100
    val model = LinearRegressionWithSGD.train(parsedData, numIterations)
    val valuesAndPreds = parsedData.map(point => {
      val prediction = model.predict(point.features)
      (point.label, prediction)
    })

    val mse = valuesAndPreds.map {
      case (v, p) => { math.pow((v - p), 2) }
    }.reduce(_ + _) / valuesAndPreds.count()
    println("训练平均误差:", mse)
    //
    //    val msg1 = valuesAndPreds.map(f => {
    //      math.pow((f._1 - f._2), 2)
    //    }).reduce(_ + _) / valuesAndPreds.count()

    sc.stop()
  }

}
