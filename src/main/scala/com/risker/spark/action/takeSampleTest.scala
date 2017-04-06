package com.risker.spark.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 返回一个固定大小的样本子集的抽样，是一个数组
  * 与sample用法相同都是抽样返回一个dataset中的num个元素，只不第二个参数换成了个数。它返回的是一个数组，看源代码可知最后调用的是take（n）
  * 第一个参数：取样是否完成更换
  * 第二个参数：返回样本的大小
  * 第三个参数：随机数生成器的种子，使用默认的每次都会是一个新的种子（随机数种子参加Java）
  * Created by hc-3450 on 2016/9/6.
  */
object takeSampleTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("takeSampleTest")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(1 to 10)
    println(data.takeSample(true,4).toBuffer)
    sc.stop()
  }

}
