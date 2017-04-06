package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hui on 2016/8/24.
  *
  *  源RDD与参数RDD相同的元素从源数据中去掉，返回一个新的RDD,不去重
  */
object SubtractTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SubtractTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(5,6,4,7,7,"qw"))
    val rdd2 = sc.parallelize(List(1,2,3,4,5,"qw"))
    val rdd3 = rdd1.subtract(rdd2)
    println(rdd3.collect().toBuffer)
    sc.stop()
  }

}
