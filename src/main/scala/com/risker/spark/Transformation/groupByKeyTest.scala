package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hc-3450 on 2016/9/5.
  */
object groupByKeyTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("groupBykeyTest")
              .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(1,2,3,4,4,5,1,1,2))
    data.map((_,1)).groupByKey().foreach(println)
    sc.stop()
  }

}
