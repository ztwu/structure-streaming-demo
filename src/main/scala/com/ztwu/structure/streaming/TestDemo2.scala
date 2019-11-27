package com.ztwu.structure.streaming

import java.sql.Timestamp

import com.ztwu.structure.streaming.sink.ConsoleSinkDemo
import com.ztwu.structure.streaming.source.{FileSourceDemo, SocketSourceDemo}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

/**
  * created with idea
  * user:ztwu
  * date:2019/11/27
  * description
  */
object TestDemo2 {

  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("TestDemo").setMaster("local[2]")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val lines = FileSourceDemo.get_input_table(spark)

    val test = lines.groupBy("sex")
      .sum("age")

    val query = ConsoleSinkDemo.get_output_taable(test).start()

    query.awaitTermination()
  }

}
