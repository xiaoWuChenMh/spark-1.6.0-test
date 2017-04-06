package com.risker.sparkES.sparkStreaming.writing

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.streaming._
import org.elasticsearch.spark.rdd.Metadata._

import scala.collection.mutable


/**
  * 给文档配置多种元数据的方案
  * Created by hc-3450 on 2016/11/24.
  */
object SettingMetadataTwo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wDstream").setMaster("local[2]")
    conf.set("es.nodes","192.168.20.128");
    conf.set("es.port","9200");
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))

    val otp = Map("iata" -> "OTP", "name" -> "1111111")
    val muc = Map("iata" -> "MUC", "name" -> "222222222")
    val sfo = Map("iata" -> "SFO", "name" -> "333333")

    val otpMeta = Map(ID -> 1, TTL -> "3h")
    val mucMeta = Map(ID -> 2, VERSION -> "23")
    val sfoMeta = Map(ID -> 3)

    val airportsRDD = sc.makeRDD(Seq((otpMeta, otp), (mucMeta, muc), (sfoMeta, sfo)))
    val microbatches = mutable.Queue(airportsRDD)
    ssc.queueStream(microbatches).saveToEsWithMeta("airports/2015")
    ssc.start()
    ssc.awaitTermination()

  }

}
