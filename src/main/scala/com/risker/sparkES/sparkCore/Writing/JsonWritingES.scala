package com.risker.sparkES.sparkCore.Writing

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._ //隐式转换

/**
  * Created by hc-3450 on 2016/11/22.
  */
object JsonWritingES {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("EsTest").setMaster("local[2]")
    //配置 es的节点信息
    sparkConf.set("es.nodes","192.168.20.128:9200");
    //elasticsearch-hadoop是否应该创建一个索引(如果索引失踪)当写入数据到Elasticsearch里或写失败时。
    sparkConf.set("es.index.auto.create", "true");

    //下面是两个条目——json,没有进行任何转换
    val json1 = """{"reason" : "business11", "airport" : "SFO11"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""
   //使用专用的saveJsonToEs方法 去索引JSON数据
    new SparkContext(sparkConf).makeRDD(Seq(json1, json2)).saveJsonToEs("spark/json-trips")
  }

}
