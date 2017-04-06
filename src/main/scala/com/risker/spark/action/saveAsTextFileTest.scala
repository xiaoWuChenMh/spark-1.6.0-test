package com.risker.spark.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 把dataSet的元素通过给定的目录在本地文件系统，HDFS或者任何其他的hadoop支持的文件系统上
  * 写入文本文件（或者文本文件的集合[多个excutor多个线程]）。Spark将在每一个元素上调用
  * toString使其转变为在文件中的一行文本
  * Created by hc-3450 on 2016/9/6.
  */
object saveAsTextFileTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("saveAsTextFileTest")
                  .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(1 to 30)
    data.saveAsTextFile("E:\\opt\\one.text")
    sc.stop()
  }

}
