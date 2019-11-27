package com.ztwu.structure.streaming

import java.sql.Timestamp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import source._
import sink._

/**
  * created with idea
  * user:ztwu
  * date:2019/11/27
  * description
  */
object TestDemo1 {

  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("TestDemo").setMaster("local[2]")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val lines = SocketSourceDemo.get_input_table(spark)
    val words = lines.as[(String, Timestamp)]
      .flatMap(line => {
        line._1.split(" ")
          .map(word => {
            (word,line._2)
          })
      })
      .toDF("word", "timestamp")

    val windowedCounts = words
      .withWatermark("timestamp", "30 seconds")
      .groupBy(window($"timestamp", "30 seconds", "15 seconds"), $"word").count()

    val query = ConsoleSinkDemo.get_output_taable(windowedCounts).start()

    query.awaitTermination()

  }
}
