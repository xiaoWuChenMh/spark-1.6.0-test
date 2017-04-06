package com.risker.sparkSql.baseTest.DataFrames

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 创建的第一个DataFrames
  * Created by hc-3450 on 2016/10/21.
  */
object FirstDataFraes {

  def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf().setAppName("firstDataFraes").setMaster("local[2]")
      val sc = new SparkContext(sparkConf)
      val sqlContext = new SQLContext(sc)
      val data = sqlContext.read.json("E:\\git\\spark-test\\src\\main\\scala\\resources\\people.json")
      data.show()
      sc.stop()
  }

}
