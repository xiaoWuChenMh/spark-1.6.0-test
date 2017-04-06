package com.risker.sparkSql.baseTest.DataSources

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hc-3450 on 2016/10/26.
  */
object HiveTable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hiveTable").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val res = hiveContext.sql("select * from mahui.employess limit 1")
    res.collect().foreach(println)
    sc.stop()
  }

}
