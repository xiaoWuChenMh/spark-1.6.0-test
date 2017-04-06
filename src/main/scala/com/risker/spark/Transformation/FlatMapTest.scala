package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 类似于map转换算子，但每一个输入项可以映射成 0或多个输出项目（所以返回一个Seq而不是一个单个的项）
  */
object FlatMapTest {

  def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("flatMapTest").setMaster("local[2]")
      val sc = new SparkContext(conf)
      val data = sc.parallelize(List("a b c","d d c a e f"))
      data.flatMap(_.split(" ")).foreach(print)
      sc.stop()

  }

}
