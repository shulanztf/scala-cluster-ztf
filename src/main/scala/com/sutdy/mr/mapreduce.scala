package com.sutdy.mr

import java.lang

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, Mapper, Reducer}
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
    values.iterator().foreach(context.write(key,_))
  }


}

