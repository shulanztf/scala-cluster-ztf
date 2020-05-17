package com.msb.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
 * @description: spark实现pageRank.
 * @author: zhaotf
 * @create: 2020-05-17 11:58
 */
object SparkPageRankLocal {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local").getOrCreate()
    //    import session._

    //    迭代几次
    val iters: Int = if (args.length > 1) {
      args(1).toInt
    } else {
      10
    }

    // 数据，KV格式
    val lines: RDD[(String, String)] = session.sparkContext.parallelize(List(("A", "B"), ("A", "C"), ("B", "A"),
      ("B", "C"),
      ("C", "A"),
      ("C", "B"),
      ("C", "D"),
      ("D", "C"),
      ("A", "E"),
      ("B", "E"),
      ("D", "E"),
      ("E", "D"),
      ("E", "A"),
      ("E", "T")))

    //    合并每个key(页面)的出链,并,加入缓存
    val links: RDD[(String, Iterable[String])] = lines.groupByKey().cache()
    //    为每个key(页面),设rank初始值
    var ranks: RDD[(String, Double)] = links.mapValues(v => 1.0)
    //    迭代收敛
    for (i <- 1 to iters) {
      val values: RDD[(Iterable[String], Double)] = links.join(ranks).values
      val contribs: RDD[(String, Double)] = values.flatMap((urls) => {
        val size = urls._1.size
        urls._1.map(url => (url, urls._2 / size)) //计算公式,出链中的每个元素的rank值=key(页面)的rank值/出链数
      })
      //      val contribs: RDD[(String, Double)] = links.join(ranks).values.flatMap({
      //        case (urls, rank) => {
      //          val size = urls.size
      //          urls.map(url => (url, rank / size))
      //        }
      //      })
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _) // 合计key(页面)的出链中所有元素的rank值, TODO,0.15是避免rank值为0,0.85是阻尼系统
    }

    val output: Array[(String, Double)] = ranks.collect() //TODO,数据拉回driver端,生产环境慎重
    output.foreach(t => println(s"${t._1},rank:${t._2}"))

    session.stop()
  }
}
