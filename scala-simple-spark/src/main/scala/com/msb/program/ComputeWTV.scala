package com.msb.program

import com.msb.util.{SegmentWordUtil, SparkSessionBase}
import org.apache.spark.ml.feature.Word2Vec


/**
 * 关键词向量
 */
object ComputeWTV {
  def main(args: Array[String]): Unit = {
    //通过SparkSessionBase创建Spark会话
    val session = SparkSessionBase.createSparkSession()
    import session.implicits._
    session.sql("use program")
    //获取节目信息，然后对其进行分词
    //    val articleDF = session.sql("select * from item_info limit 20")
    val articleDF = session.table("item_info").limit(1000)
    val seg = new SegmentWordUtil()
    articleDF.repartition()
    val words_df = articleDF.rdd.mapPartitions(seg.segeFun).toDF("item_id", "words")
//    words_df.show(5,false)//数据显示，不折行

    /**
     * vectorSize: 词向量长度
     * minCount：过滤词频小于5的词
     * windowSize：window窗口大小
     */
    val w2v = new Word2Vec
    w2v.setInputCol("words")
    w2v.setOutputCol("model")
    w2v.setVectorSize(128)
    w2v.setMinCount(3)
    w2v.setWindowSize(5)

    val w2vModel = w2v.fit(words_df)
    w2vModel.write.overwrite().save("hdfs://hserver134:9000/recommond_program/models/w2v.model")
    session.read.parquet("hdfs://hserver134:9000/recommond_program/models/w2v.model/data/*").show(false)
    session.close()
  }
}
