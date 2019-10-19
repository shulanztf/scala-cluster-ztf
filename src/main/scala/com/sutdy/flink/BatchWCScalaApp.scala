package com.sutdy.flink

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * flink-scala批作业
  */
class BatchWCScalaApp {

}

object BatchWCScalaApp {

  def main(args: Array[String]): Unit = {
    val input = "file:///data/flink/text-group-1"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(input)


    import org.apache.flink.api.scala._
    text.flatMap(_.toLowerCase.split("\t")).filter(_.nonEmpty).map((_, 1))
      .groupBy(0)
      .sum(1).print()

  }

}

