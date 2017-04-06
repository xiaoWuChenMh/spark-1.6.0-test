package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  *      当在（k,v）对的数据集上调用此函数是，会返回一个（k,v）对的数据集对
  * 每一个键的值在哪里聚合通过使用给定的reduce 函数来决定，类型必须是（V,V）=>V。
  * 像在groupByKey一样，减少任务的数据通过一个可选的第二个参数可以来配置。
  *      reduceByKey用于对每个key对应的多个value进行merge操作，最重要的是它
  * 能够在本地先进行merge操作，并且merge操作可以通过函数自定义。
  *
  * Created by hc-3450 on 2016/9/2.
  */
object reduceByKeyTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("reduceByKeyTest")
        .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(1,2,3,4,4,5,1,1,2))
    data.map((_,1)).reduceByKey(_+_).foreach(print)
    sc.stop()

  }

}
