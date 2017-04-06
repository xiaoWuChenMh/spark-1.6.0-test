package com.risker.sparkSql.baseTest.DataSources.commonly

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 一般的载入/保存方法
  * 输入和输出文件均为 .parquet 后缀的文件
  * Created by hc-3450 on 2016/10/25.
  */
object GenericOperation {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataSources1")
      .setMaster("local[2]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    //载入指定文件的内容
    val df =sqlContext.read.load("E:\\git\\spark-test\\src\\main\\scala\\resources\\users.parquet")

    //key 是 name 和favorite_color的value在控制台输出
    df.select("name", "favorite_color").collect().foreach(println)

    //key 是 name 和favorite_color的value输出到指定文件
    df.select("name", "favorite_color").write.save("E:\\git\\spark-test\\src\\main\\scala\\resources\\namesAndFavColors.parquet")

    sc.stop()
  }

}
