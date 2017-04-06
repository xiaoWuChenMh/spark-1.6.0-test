package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 当有两个KV的dataset(K,V)和(K,W)，返回的是(K,(V,W))的dataset,numTasks为并发的任务数.
  * 注意它会以第一个RDD的每一个元素作为关键元素去和第二个rDD进行join
  * 外部连接支持通过leftOuterJoin rightOuterJoin,fullOuterJoin。
  * Created by hc-3450 on 2016/9/5.
  */
object joinTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("joinTest")
                .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(List((1,"A"),(2,"B"),(3,"C"),(1,"D")))
    val b= sc.parallelize(List((1,"a"),(2,"b"),(3,"c"),(1,"d")))
    a.join(b).foreach(println)
   // a.leftOuterJoin(b).foreach(println)
    sc.stop()
  }

}
