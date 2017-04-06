package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 笛卡尔积就是m*n
  * Created by hc-3450 on 2016/9/6.
  */
object cartesianTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("cartesianTest")
                .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(List("a","b")).map(x=>x)
    val b = sc.parallelize(List(1,2)).map(x=>x)
    val rs = a.cartesian(b)
    rs.foreach(print)
    sc.stop()
    //(a,1)(a,2)(b,1)(b,2)
  }

}
