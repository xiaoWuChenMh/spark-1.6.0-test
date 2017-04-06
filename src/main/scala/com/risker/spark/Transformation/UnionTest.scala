package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  *    union算子的作用：
  *         两个RDD进行合并，不去重。
  */
object UnionTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("collectTest").setMaster("local[2]")

    val sc =new SparkContext(conf)

    val rdd6 = sc.parallelize(List(5,6,4,7))
    val rdd7 = sc.parallelize(List(1,2,3,4))
    val rdd8 = rdd6.union(rdd7)
    val result = rdd8.sortBy(x=>x)
    println(result.collect().toBuffer)
    result.foreach(println)
    sc.stop()
  }


}
