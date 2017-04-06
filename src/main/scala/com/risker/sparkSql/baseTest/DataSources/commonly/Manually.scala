package com.risker.sparkSql.baseTest.DataSources.commonly

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hc-3450 on 2016/10/25.
  */
object Manually {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("DataSources2")
      .setMaster("local[2]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("json").load("E:\\git\\spark-test\\src\\main\\scala\\resources\\people.json")
    df.select("name","age").collect().foreach(println)
    df.select("name","age").write.format("parquet").save("E:\\git\\spark-test\\src\\main\\scala\\resources\\namesAndAges.parquet")

    sc.stop()
  }

}
