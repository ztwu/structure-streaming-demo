package com.ztwu.structure.streaming.source

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * created with idea
  * user:ztwu
  * date:2019/11/27
  * description
  */
object FileSourceDemo {

  def get_input_table(ssc:SparkSession): DataFrame = {

    val schema = StructType(
      StructField("name",StringType)
        ::StructField("age",IntegerType)
        :: StructField("sex",StringType)
        :: Nil)

    val source = ssc
      .readStream
      // Schema must be specified when creating a streaming source DataFrame.
      .schema(schema)
      // 每个trigger最大文件数量
      .option("maxFilesPerTrigger",100)
      // 是否首先计算最新的文件，默认为false
      .option("latestFirst",value = true)
      // 是否值检查名字，如果名字相同，则不视为更新，默认为false
      .option("fileNameOnly",value = true)
      .csv("data/*.csv")

    return source
  }

}
