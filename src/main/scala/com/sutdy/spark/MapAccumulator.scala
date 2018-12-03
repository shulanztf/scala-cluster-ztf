package com.sutdy.spark

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * https://www.cnblogs.com/namhwik/p/6225153.html Spark2.0自定义累加器
  */
class MapAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, String]] {
  private val mapAccumulator = mutable.Map[String, String]()


  override def isZero: Boolean = {
    mapAccumulator.isEmpty
  }

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, String]] = {
    val newMapTokenizer = new MapAccumulator
    mapAccumulator.foreach(f => {
      newMapTokenizer.add(f)
    })
    newMapTokenizer
  }

  /**
    * 将累加器进行重置
    */
  override def reset(): Unit = {
    mapAccumulator.clear()
  }

  /**
    * 向累加器中添加另一个值
    *
    * @param k
    */
  override def add(k: (String, String)): Unit = {
    val key = k._1
    val value = k._2
    if (!mapAccumulator.contains(key)) {
      mapAccumulator += (key -> value)
    } else if (mapAccumulator.get(key).get != value) {
      mapAccumulator += key -> (mapAccumulator.get(key).get + "||" + value)
    }
  }

  /**
    * 合并另一个类型相同的累加器
    *
    * @param other
    */
  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, String]]): Unit = other match {
    case map: MapAccumulator => {
      other.value.foreach(x => {
        if (!this.value.contains(x._1)) {
          this.add(x)
        } else {
          x._2.split("\\|\\|").foreach(f => {
            if (!this.value.get(x._1).get.split("\\|\\|").contains(f)) {
              this.add(x._1, f)
            }
          })
        }
      })
    }
    case _ => {
      throw new UnsupportedOperationException(s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  /**
    * 取值
    *
    * @return
    */
  override def value: mutable.Map[String, String] = {
    mapAccumulator
  }
}
