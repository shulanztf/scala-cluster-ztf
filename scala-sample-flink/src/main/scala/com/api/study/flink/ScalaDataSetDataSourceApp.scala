package com.sutdy.flink

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  *
  */
class ScalaDataSetDataSourceApp {

}

/**
  *
  */
object ScalaDataSetDataSourceApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    readText(env)
    print("aaa..................")
  }

  def readText(env:ExecutionEnvironment): Unit = {
    val filePath = "file:///data/flink/text-group-1"
    env.readTextFile(filePath).print()
  }

}
