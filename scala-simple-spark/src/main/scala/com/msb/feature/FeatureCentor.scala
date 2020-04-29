package com.msb.feature

import com.msb.util.{HBaseUtil, PropertiesUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.ml.linalg.SparseVector


/**
 *
 */
object FeatureCentor {

  def updateFeatureCentor={
    val features = FeaturesFactory.getLRFeatures
    features.show(5,false)//数据显示，不折行
    features.printSchema()

    /**
     * root
     * |-- userID: string (nullable = true)
     * |-- itemID: integer (nullable = false)
     * |-- duration: long (nullable = false)
     * |-- program_features: vector (nullable = true)
     * |-- province_Vector: vector (nullable = true)
     * |-- city_Vector: vector (nullable = true)
     * |-- userLabel_Vector: vector (nullable = true)
     * |-- label: integer (nullable = false)
     * |-- features: vector (nullable = true)
     */
    val tableName = PropertiesUtils.getProp("user.item.feature.centor")
    features.rdd.foreachPartition(partition => {
      val conf = HBaseUtil.getHBaseConfiguration()
      //      conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
      val htable = HBaseUtil.getTable(conf,tableName)
      partition.foreach(row => {
        val userID = row.getAs[String]("userID")
        val itemID = row.getAs[Int]("itemID")
        val features = row.getAs[SparseVector]("features")
        val put = new Put(Bytes.toBytes(userID+":"+itemID))
        put.addColumn(Bytes.toBytes("feature"), Bytes.toBytes("feature"), Bytes.add(Bytes.toByteArrays(features.toDense.toArray.map(_.toString))))
        htable.put(put)
      })
      htable.close()
    })
  }

  def main(args: Array[String]): Unit = {
    FeatureCentor.updateFeatureCentor
  }
}
