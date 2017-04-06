package com.risker.sparkES.sparkStreaming.writing

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.streaming._

import scala.collection.mutable

/**
  * 简单的设定元数据（id）
  * id 不会成为 字段
  * Created by hc-3450 on 2016/11/24.
  */
object SettingMetadata {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wDstream").setMaster("local[2]")
    conf.set("es.nodes","192.168.20.128");
    conf.set("es.port","9200");
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))

    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    val airportsRDD = sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))
    val microbatches = mutable.Queue(airportsRDD)

    ssc.queueStream(microbatches).saveToEsWithMeta("airports/2015")
    ssc.start()
    ssc.awaitTermination()
  }

}
