package com.risker.Test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hui on 2016/5/9.
  */
object WorkCount {
  //spark submit通过反射调用我自定义的app的main方法
  def main(args: Array[String]) {
    //加载默认配置
    val conf = new SparkConf().setAppName("df").setMaster("local[2]")
    //TODO 创建非常重要的sparkContext,他是通往集群的入口
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("F:\\word\\word1.txt").flatMap(_.split(" "))
    val rdd2 =rdd1.map((_,1)).reduceByKey(_+_)
    rdd2.saveAsTextFile("")
    sc.stop()
  }
}
