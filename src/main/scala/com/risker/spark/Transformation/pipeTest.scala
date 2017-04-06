package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hc-3450 on 2016/9/6.
  */
object pipeTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("pipeTest")
                    .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = List("hi", "hello", "how", "are", "you")
    val dataRDD = sc.makeRDD(data)
    val scriptPath = "/home/gt/spark/bin/echo.sh"
    val pipeRDD = dataRDD.pipe(scriptPath)
    print(pipeRDD.collect())
    sc.stop()
  }

}
