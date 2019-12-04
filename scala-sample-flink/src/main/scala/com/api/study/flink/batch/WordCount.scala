package com.api.study.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.slf4j.LoggerFactory

/**
  * @ClassName: WordCount
  * @Author: zhaotf
  * @Description:
  * @Date: 2019/12/4 0004 
  */
class WordCount {

}

/**
  * https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/batch/
  */
object WordCount {
  val logger = LoggerFactory.getLogger(WordCount.getClass)

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val text = env.fromElements("Who's there?",
      "I think I hear them. Stand, ho! Who's there?")
    val counts = text.flatMap(x => {
      x.toLowerCase().split("\\W+").filter(_.nonEmpty)
    }).map((_, 1))
      .groupBy(0)
      .sum(1)
    counts.print()
  }
}
