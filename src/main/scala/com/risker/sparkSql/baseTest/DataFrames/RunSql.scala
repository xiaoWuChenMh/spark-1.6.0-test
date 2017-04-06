package com.risker.sparkSql.baseTest.DataFrames

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by hc-3450 on 2016/10/24.
  */
object RunSql {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("firstDataFraes").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.read.json("E:\\git\\spark-test\\src\\main\\scala\\resources\\people.json")
    //DataFrame注册成表
    data.registerTempTable("people")
    val df = sqlContext.sql("SELECT * FROM people")
    df.show()
    sc.stop()
  }

}
