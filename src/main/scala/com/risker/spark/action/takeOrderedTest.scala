package com.risker.spark.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回RDD的n个元素，这些返回的元素使用自然秩序排列（升序）或者你也可以定制的比较器（实现ordering特质）。
  * Created by hc-3450 on 2016/9/6.
  */
object takeOrderedTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("takeOrderedTest")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(3,65,6,24,4,3))
    println(data.takeOrdered(6).toBuffer)
    sc.stop()
  }
}
