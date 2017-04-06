package com.risker.spark.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回的是dataset中的element的个数
  * Created by hc-3450 on 2016/9/6.
  */
object countTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("countTest")
              .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(1 to 100)
    val rs = data.count()
    println(rs)
    sc.stop()
      //100
  }

}
