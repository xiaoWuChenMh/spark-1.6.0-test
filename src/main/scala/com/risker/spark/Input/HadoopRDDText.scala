package com.risker.spark.Input

import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hui on 2016/8/29.
  */
object HadoopRDDText {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HadoopRDDTest").setMaster("local[2]")
    val sc = new SparkContext(conf)

    sc.stop()

  }

}
