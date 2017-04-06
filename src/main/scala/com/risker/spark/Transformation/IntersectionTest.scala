package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hui on 2016/8/24.
  */
object IntersectionTest {

  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("IntersectionTest").setMaster("local[2]")
      val sc = new SparkContext(conf)
      val rdd1 = sc.parallelize(List(5,6,4,7,"qw"))
      val rdd2 = sc.parallelize(List(1,2,3,4,5,"qw"))
      val rdd3 = rdd1.intersection(rdd2)  // 求交集
       println(rdd3.collect().toBuffer)   //输出运算结果
     sc.stop()
  }

}
