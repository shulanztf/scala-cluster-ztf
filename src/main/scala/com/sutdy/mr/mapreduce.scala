package com.sutdy.mr

import java.lang

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, Mapper, Reducer}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import scala.collection.JavaConversions._

/**
  * https://blog.csdn.net/hopeatme/article/details/52655576 Scala 开发简单mapreduce 程序
  */
class mapreduce {

}

/**
  * map类
  */
class SmapleFileMapper extends Mapper[LongWritable, Text, Text, Text] {
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    val inputSplit: InputSplit = context.getInputSplit
    val filename: String = inputSplit.asInstanceOf[FileSplit].getPath.getName
    val word = new Text()
    word.set(filename)
    context.write(word, value)
  }

}

/**
  * reduce类
  */
class SmallFileReducer extends Reducer[Text, Text, Text, Text] {

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    values.iterator().foreach(context.write(key, _))
  }
}

/**
  * https://blog.csdn.net/dkl12/article/details/80943341 Spark获取当前分区的partitionId
  */
object wordConumtLoacl1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordConumtLoacl1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val textRDD = sc.textFile("/data/spark/source/word-count-3.txt")

    //
    val filterRDD = textRDD.flatMap(f => {
      println("flatMap", TaskContext.getPartitionId(), TaskContext.get.partitionId, f)
      f.split(" ")
    }).filter(f => {
      println("filter", f)
      StringUtils.isNotBlank(f)
    })
    //
    val mapRDD = filterRDD.map(f => {
      println("map", TaskContext.getPartitionId(), TaskContext.get.partitionId, f)
      (f, 1)
    })
    //    filterRDD.groupby
    mapRDD.reduceByKey(_ + _).collect().foreach(f => {
      println("end", f)
    })


  }

}




































