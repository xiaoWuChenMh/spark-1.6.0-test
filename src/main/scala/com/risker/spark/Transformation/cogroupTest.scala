package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 当有两KV的dataset(K,V)和(K,W)，返回的是(K,Seq[V],Seq[W])的dataset,
  * numTasks为并发的任务数
  * 此函数还有多个重载的方法，otherDataset可以是1到3个。
  * Created by hc-3450 on 2016/9/6.
  */
object cogroupTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("cogroupTest")
                .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(List("basketball")).map(("interest",_))
    val b = sc.parallelize(List("E-games")).map(("interest",_))
    val c = sc.parallelize(List("football")).map(("interest",_))
    val newDataSet = a.cogroup(b,c,c)
    newDataSet.foreach(println)
    sc.stop()
    //(interest,(CompactBuffer(basketball),CompactBuffer(E-games),CompactBuffer(football)))
  }

}
