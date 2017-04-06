package com.risker.sparkES.sparkCore.Writing

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * 显示的调用某个方法，把数据导入es，同时可以配置文档的id
  * 或者其他的fields 元数据比如 ttl（生存时间） 或者 timestamp（时间戳）
  * Created by hc-3450 on 2016/11/22.
  */
object sparkWritingEsThree {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("EsTest").setMaster("local[2]")
    //配置 es的节点信息
    sparkConf.set("es.nodes","192.168.20.128:9200");
    //elasticsearch-hadoop是否应该创建一个索引(如果索引失踪)当写入数据到Elasticsearch里或写失败时。
    sparkConf.set("es.index.auto.create", "true");
    //创建一个SparkContext对象
    val sc = new SparkContext(sparkConf)
    //定义一个名为Trip的case class
    case class Trip(departure: String, arrival: String,id:String)
    val upcomingTrip = Trip("OTP", "SFO","1")
    val lastWeekTrip = Trip("MUC", "OTP","2")
    //创建一个RDD,把Trip实例导入
    val rdd = sc.makeRDD(Seq(upcomingTrip,lastWeekTrip))
    //使用EsSpark参数是 makeRDD生产的RDD 和 index
    EsSpark.saveToEs(rdd, "spark/docs", Map("es.mapping.id" -> "id"))

  }

}
