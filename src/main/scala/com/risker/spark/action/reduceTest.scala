package com.risker.spark.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * reduce将RDD中元素两两传递给输入函数，同时产生一个新的值，
  * 新产生的值与RDD中下一个元素再被传递给输入函数直到最后只有一个值为止。
  * Created by hc-3450 on 2016/9/6.
  */
object reduceTest {

  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("reduceTest")
                  .setMaster("local[2]")
     val sc = new SparkContext(conf)
     val data = sc.parallelize(1 to 10)
     val rs = data.reduce((x,y)=>x+y)
     println(rs)
     sc.stop()
  }

}
