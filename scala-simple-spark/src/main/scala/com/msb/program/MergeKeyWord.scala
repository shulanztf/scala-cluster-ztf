package com.msb.program

import com.msb.util.SparkSessionBase
import org.apache.spark.sql.SaveMode


/**
 * 合并关键词
 */
object MergeKeyWord {
  def main(args: Array[String]): Unit = {
    val session = SparkSessionBase.createSparkSession()
    import session.implicits._
    session.sql("use tmp_program")
    /**
     * +-------+--------------------+
     * |item_id|             keyword|
     * +-------+--------------------+
     * | 159131|            [音乐, 性感]|
     * | 158531|          [乐队, 歌, 最]|
     * | 159356|              [中, 李]|
     * | 158306|         [乐队, 最, 演唱]|
     * 合并 作为关键词  学费！！！
     */
    val sqlText = "" +
      "SELECT w.item_id, collect_set(w.keywords) AS keyword1, collect_set(k.word) AS keyword2 " +
      "  FROM keyword_tr w " +
      "  JOIN keyword_tfidf k ON (w.item_id = k.item_id) " +
      " GROUP BY w.item_id"
    val mergeDF = session.sql(sqlText)
    mergeDF.rdd.map(row => {
      val itemID = row.getAs[Long]("item_id")
      val keyword1 = row.getAs[Seq[String]]("keyword1")
      val keyword2 = row.getAs[Seq[String]]("keyword2")
      val keywords:Array[String] = keyword1.union(keyword2).distinct.toArray
      (itemID, keywords)
    }).toDF("item_id", "keyword")
      .write
      .mode(SaveMode.Overwrite)
      .insertInto("item_keyword")// TODO,hive item_keyword.keyword 字段类型调整，字符串转seq
  }
}
