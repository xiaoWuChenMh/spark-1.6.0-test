package com.risker.spark.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * foreach用于遍历RDD,将函数f应用于每一个元素。
  * 但要注意，如果对RDD执行foreach，只会在Executor端有效，而并不是Driver端。
  * 什么情况下使用此函数：
  *   1、更新蓄电池
  *   2、与外部存储系统进行交互。
  *需要注意：
  *   修改变量时除了蓄电池以外的foreach()可能会导致未定义的行为。因为闭包
  * Created by hc-3450 on 2016/9/6.
  */
object foreachTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("foreachTest")
                  .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List("your","wo","her","she","wo","she","wo"))
              .map((_,1)).reduceByKey(_+_).foreach(print)
    sc.stop()
    //(wo,3)(she,2)(your,1)(her,1)
  }

}
