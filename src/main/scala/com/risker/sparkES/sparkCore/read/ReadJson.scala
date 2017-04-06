package com.risker.sparkES.sparkCore.read

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
/**
  * Created by hc-3450 on 2016/11/22.
  */
object ReadJson {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("EsTest").setMaster("local[2]")
    sparkConf.set("es.nodes","192.168.20.128");
    sparkConf.set("es.port","9200");
    sparkConf.set("es.index.auto.create", "true");
    val sc = new SparkContext(sparkConf)
    val query = """ {"query": { "match_all": {} }}""" //yes
    val query1 = """ {"query": { "match_all": {} },"size": 1}""" //size 这种语法不支持
    val rdd1 = sc.esJsonRDD("spark/docs",query)//是懒加载的
    val rdd2 = sc.esJsonRDD("spark/docs",query)//是懒加载的
    //(2,{"departure":"MUC","arrival":"OTP","id":"2"}) 格式：RDD[(String, String)] 解释RDD[(id,文档 )]
    //如何id是自定义的会存在文档中，否则不存在 如：(DX9mzquQRambXSMjbJIZQw,{"departure":"OTP","arrival":"SFO"})
    rdd1.foreach(println)
    println("=========")
    sc.stop()

  }

}
