package com.sutdy.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.sql.SparkSession

/**
  * 机器学习及SparkMLlib简介
  * http://www.cnblogs.com/shishanyuan/p/4747761.html
  */
class LocalSparkMllib {

}

/**
  * 3.2.2 回归算法
  * 导入训练数据集，将其解析为带标签点的RDD，使用 LinearRegressionWithSGD 算法建立一个简单的线性模型来预测标签的值，最后计算均方差来评估预测值与实际值的吻合度。
  */
object LocalLinearRegressionWithSGD {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val conf = SparkSession.builder().appName("LocalLinearRegressionWithSGD").master("local[2]").getOrCreate().sparkContext
    val data = conf.textFile("D:\\data\\spark\\data\\class8\\lpsa.data")
    val parsedData = data.map(line => {
      val parts = line.split(",")
      //      LabeledPoint(parts(0).toDouble,parts(1).split(" ").map(x=>x.toDouble).toArray)
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(x => x.toDouble)))
    })

    //设置迭代次数并进行训练
    val numIterations = 20
    val model = LinearRegressionWithSGD.train(parsedData, numIterations)
    //    LinearRegression.
    //    LBFGS
    // 统计回归错误的样本比例
    val valuesAndPreds = parsedData.map(point => {
      val prediction = model.predict(point.features)
      (point.label, prediction)
    })
    val mse = valuesAndPreds.map {
      case (v, p) => math.pow((v - p), 2)
    }.reduce(_ + _) / valuesAndPreds.count()
    println("training Mean Squared Error = " + mse)
    conf.stop()
  }

}

/**
  * 3.2.3 聚类算法
  * 导入训练数据集，使用 KMeans 对象来将数据聚类到两个类簇当中，所需的类簇个数会被传递到算法中，然后计算集内均方差总和 (WSSSE)，可以通过增加类簇的个数 k 来减小误差。 实际上，最优的类簇数通常是 1，因为这一点通常是WSSSE图中的 “低谷点”。
  */
object LocalKMeans {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = SparkSession.builder().appName("LocalKMeans").master("local[2]").getOrCreate().sparkContext
    // 加载和解析数据文件
    val data = conf.textFile("D:\\data\\spark\\data\\class8\\kmeans_data.txt")
    //    val parsedData = data.map(_.split(" ").map(_.toDouble))
    val parsedData = data.map((line: String) => {
      Vectors.dense(line.split(" ").map(_.toDouble))
    })
    // 设置迭代次数、类簇的个数
    val numIterations = 20
    val numClusters = 2
    // 进行训练
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    // 统计聚类错误的样本比例
    val wssse = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + wssse)
    conf.stop()
  }
}

/**
  * 3.2.4 协同过滤
  * 导入训练数据集，数据每一行由一个用户、一个商品和相应的评分组成。假设评分是显性的，使用默认的ALS.train()方法，通过计算预测出的评分的均方差来评估这个推荐模型。
  */
object LocalRating {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = SparkSession.builder().appName("LocalRating").master("local[2]").getOrCreate().sparkContext
    // 加载和解析数据文件
    val data = conf.textFile("D:\\data\\spark\\data\\class3\\test.txt")
    val ratings = data.map(_.split(",") match {
      case Array(user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble)
    })
    // 设置迭代次数
    val numIterations = 20
    val model = ALS.train(ratings, 1, 20, 0.01)
    // 对推荐模型进行评分
    val usersProducts = ratings.map { case Rating(user, product, rate) => (user, product) }
    val predictions = model.predict(usersProducts).map {
      case Rating(user, product, rate) => ((user, product), rate)
    }
    val ratesAndPreds = ratings.map {
      case Rating(user, product, rate) => ((user, product), rate)
    }.join(predictions)
    val mse = ratesAndPreds.map {
      case ((user, product), (r1, r2)) => math.pow((r1 - r2), 2)
    }.reduce(_ + _) / ratesAndPreds.count()
    println("Mean Squared Error = ", mse)
    conf.stop()
  }
}
