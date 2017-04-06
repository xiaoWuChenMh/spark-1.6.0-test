package com.risker.sparkES.sparkCore.read

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by hc-3450 on 2016/11/22.
  */
object baseData {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("EsTest").setMaster("local[2]")
    sparkConf.set("es.nodes","192.168.20.128");
    sparkConf.set("es.port","9200");
    sparkConf.set("es.index.auto.create", "true");
    val sc = new SparkContext(sparkConf)
    val query = """ {"query": { "match_all": {} }}"""
    val rdd1 = sc.esRDD("spark/people",query)//是懒加载的
 //   val rdd2 = sc.esRDD("spark/docs")//是懒加载的 ,
    //(1,Map(departure -> OTP, arrival -> SFO, id -> 1)) 格式 RDD[(String, Map)] 解释RDD[(id,文档 )]
    //如何id是自定义的会存在文档中，否则不存在 如：(-vkBcVr1T8CQbOx8cBjSzg,Map(departure -> OTP, arrival -> SFO))
    rdd1.foreach(println)
    println("=========")
    sc.stop()

  }

}
