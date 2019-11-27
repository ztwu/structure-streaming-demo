package com.ztwu.structure.streaming.sink

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}

/**
  * created with idea
  * user:ztwu
  * date:2019/11/27
  * description
  */
object ConsoleSinkDemo {

  def get_output_taable(dataframe:DataFrame): DataStreamWriter[Row] = {
    val query = dataframe
      .writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime(5000))
      .option("truncate", "false")
    return query

  }

}
