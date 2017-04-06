package com.risker.sparkES

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hc-3450 on 2016/11/22.
  */
object template {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("EsTest").setMaster("local[2]")
    sparkConf.set("es.nodes","192.168.20.128:9200");
    sparkConf.set("es.index.auto.create", "true");
    val sc = new SparkContext(sparkConf)


  }

}
