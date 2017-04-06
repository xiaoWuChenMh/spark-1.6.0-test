package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  *  map(func):对调用map的RDD数据集中的每个element都使用func，
  *  然后返回一个新的RDD,这个返回的数据集是分布式的数据集
  */
object MapTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapTest").setMaster("local[2]")
    val sc  = new SparkContext(conf)
    val data = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    val info = data.map(x=>{"NO:"+x+"   "})
    val d = info.map(x=>x+1)
    val c = info.map(x=>x+2)
     d.take(8).foreach(print)
    println("   ")
    c.take(8).foreach(print)
    sc.stop()
  }

}
