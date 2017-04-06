package com.risker.spark.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回的是一个数组是dataset中的前n个元素
  * Created by hc-3450 on 2016/9/6.
  */
object takeTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("takeTest")
                  .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(30 to 40)
    println(data.take(3).toBuffer)
    sc.stop()
  }
}
