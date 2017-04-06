package com.risker.spark.other

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 迭代器
  * Created by hc-3450 on 2016/9/6.
  */
object iteratorTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("iteratorTest")
                  .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List("1","df","dfxd"))
    data.foreachPartition(partition=>{
      val a = partition.map((_,1))
      val b = a.map(x=>((x._1,"test")))
      b.foreach(print)
      println()
      a.foreach(print)

    })

  }

}
