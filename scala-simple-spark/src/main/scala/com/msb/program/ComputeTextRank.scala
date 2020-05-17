package com.msb.program

import com.msb.algorithm.TextRank
import com.msb.util.{SegmentWordUtil, SparkSessionBase}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * 重头节目关键词的TextRank
 */
object ComputeTextRank {

  def main(args: Array[String]): Unit = {
    //通过SparkSessionBase创建Spark会话
    val session = SparkSessionBase.createSparkSession()
    session.sql("use program")
    //获取节目信息，然后对其进行分词
    //    val articleDF = session.sql("select * from item_info limit 20")
    val articleDF = session.table("item_info").limit(1000)
    val seg = new SegmentWordUtil()
    //[itemID,[w1,w2,w3......]],节目id,描述、标题、名字的拼接串
    val wordsRDD: RDD[(Long, List[String])] = articleDF.rdd.mapPartitions(seg.segeFun)

    //计算每个节目 每个单词的TR值
    val tralgm = new TextRank()
    //transform构建图,节目id,textRank需要的数据(KV(V为链表)对)
    val transformGraphRDD: RDD[(Long, mutable.HashMap[String, mutable.HashSet[String]])]
    = wordsRDD.map(x => (x._1, tralgm.transform(x._2)))
    // 节目id,节目下所有单词的textRank值(KV对)
    val rankRDD:RDD[(Long,mutable.HashMap[String, Double])] = transformGraphRDD.map(x => (x._1, tralgm.rank(x._2)))
    //    rankRDD.foreach(println)

    /**
     * 将每个节目 每个单词的TR值与对应的单词的IDF相乘
     * （1）创建广播变量  将idf_keywords（word idf）表数据  作为广播变量
     * （2）遍历sortByRankRDD 匹配单词，TR*IDF
     */
    val word2IDFMap = mutable.Map[String, Double]()
    session.table("tmp_program.keyword_idf").rdd.collect().foreach(row => {
      word2IDFMap += ((row.getAs[String]("keywords"), row.getAs[Double]("idf")))
    })
    val word2IDFBroad = session.sparkContext.broadcast(word2IDFMap)

    //将每篇文章中每个单词的tr*对用的IDF值  作为筛选关键词的依据
    val keyWordsWithWeightsRDD = rankRDD.map(data => {
      val itemID = data._1
      val word2TR = data._2
      val word2IDFMap = word2IDFBroad.value
      val list = new ListBuffer[(Long, String, Double)]
      //itemID   [word1：组合权重1，word2：组合权重2 .....]
      val word2Weights = word2TR.map(t => {
        val word = t._1
        val tr = t._2
        var weights = 0d
        if (word2IDFMap.contains(word)) {
          //word2IDFMap(word)  idf值
          weights = word2IDFMap(word) * tr
        } else {
          weights = tr
        }
        //单词对应的组合权重
        (word, weights)
      })
      (itemID, word2Weights)
    })

    //根据混合的weight值排序，选择topK个单词
    val sortByWeightRDD = keyWordsWithWeightsRDD
      //      .filter(_._2.size > 10)
      .map(x => (x._1, sortByWeights(x._2)))
      .flatMap(explode)

    //keyWordsWithWeightsRDD转成DF
    import session.implicits._
    val word2WeightsDF = sortByWeightRDD.toDF("item_id", "keywords", "weight")
    session.sql("use tmp_program")
    word2WeightsDF.write.mode(SaveMode.Overwrite).saveAsTable("keyword_tr")

    /**
     * create table keyword_tr(
     * item_id Int comment "index",
     * word STRING comment "word",
     * tr Double comment "idf"
     * )
     * COMMENT "keyword_tr"
     * row format delimited fields terminated by ','
     * LOCATION '/user/hive/warehouse/tmp_program.db/keyword_tr';
     */
    session.close()
  }

  def explode(data: (Long, Map[String, Double])) = {
    val itemID = data._1
    val ds = data._2
    val list = new ListBuffer[(Long, String, Double)]
    for (elem <- ds) {
      list += ((itemID, elem._1, elem._2))
    }
    list.iterator
  }

  def sortByWeights(doc: mutable.HashMap[String, Double]) = {
    val mapDoc = doc.toSeq
    val reverse = mapDoc.sortBy(-_._2).take(10).toMap
    reverse
  }
}
