package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  *      对调用filter的RDD数据集中的每个元素都使用func，
  * 然后返回一个包含使func为true的元素构成的新的分布式数据集（RDD）
  */
object FilterTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("filterTest").setMaster("local[2]")
    val sc = new SparkContext(conf);
    val data = sc.parallelize(List(1,2,9,3,6,7,4,6,8))
    data.filter(x=>x>5).map(x=>x+" ").foreach(print)
    sc.stop()

  }

}
