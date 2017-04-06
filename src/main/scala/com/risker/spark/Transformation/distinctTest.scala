package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 去重
  */
object distinctTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("distnctTest")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(1,2,3,4,4,3))
    data.distinct(2).map(x=>x+" ").foreach(print)
    sc.stop()
  }

}
