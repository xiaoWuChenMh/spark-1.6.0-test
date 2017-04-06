package com.risker.spark.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回的是dataSet中的第一个元素（类似于take(1)）
  * Created by hc-3450 on 2016/9/6.
  */
object firstTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("firstTest")
                  .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List("wo",1,23,"34","hell"))
    println(data.first())
    sc.stop()
  }
}
