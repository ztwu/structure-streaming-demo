package com.ztwu.structure.streaming.source

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * created with idea
  * user:ztwu
  * date:2019/11/27
  * description
  */
object SocketSourceDemo {

  def get_input_table(ssc:SparkSession): DataFrame = {
    val lines = ssc.readStream
      .format("socket")
      .option("host", "127.0.0.1")
      .option("port", 9999)
      .option("includeTimestamp", true)
      .load()
    return lines
  }

}
