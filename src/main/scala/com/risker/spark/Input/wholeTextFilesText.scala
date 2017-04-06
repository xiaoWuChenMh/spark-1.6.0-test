package com.risker.spark.Input

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hui on 2016/8/25.
  */
object wholeTextFilesText {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wholeTextFilesText").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val date = sc.wholeTextFiles("F:\\opt\\sparkApp\\log\\mysqlLog")

    date.foreachPartition(f=>{
      f.foreach(x=>{
        println(x)
      })
    })
    sc.stop()

  }

}
