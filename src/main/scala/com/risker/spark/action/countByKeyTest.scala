package com.risker.spark.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hc-3450 on 2016/9/6.
  */
object countByKeyTest {

  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("countByKeyTest")
                    .setMaster("local[2]")
      val sc = new SparkContext(conf)
      val data = sc.parallelize(List(("a","ok"),("b","ok"),("c","ok"),("a","ok")))
      println(data.countByKey().toBuffer)
      sc.stop()

  }

}
