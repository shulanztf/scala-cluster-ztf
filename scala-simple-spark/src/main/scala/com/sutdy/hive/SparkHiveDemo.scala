package com.sutdy.hive

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  */
object SparkHiveDemo {

  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("hive.metastore.uris", "thrift://hserver134:9083")
      .enableHiveSupport()
      .getOrCreate()

//    session.sql("use program")
//    val df = session.read.table("program.table")
//    df.show(false)

    val data:DataFrame = session.sql("select * from program.item_info limit 20")
    data.show(3,false)

    session.close()
  }

}
