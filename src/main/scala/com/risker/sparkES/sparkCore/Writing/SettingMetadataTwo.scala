package com.risker.sparkES.sparkCore.Writing

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.Metadata._

/**
  * 给文档配置多种元数据的方案
  * Created by hc-3450 on 2016/11/22.
  */
object SettingMetadataTwo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("EsTest").setMaster("local[2]")
    sparkConf.set("es.nodes","192.168.20.128:9200");
    sparkConf.set("es.index.auto.create", "true");
    val sc = new SparkContext(sparkConf)
    val otp = Map("iata" -> "OTP", "name" -> "Otopeni2")
    val muc = Map("iata" -> "MUC", "name" -> "Munich2")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran2")

    //每一个文档的元数据
    val otpMeta = Map(ID -> 1, TTL -> "3h")
    val mucMeta = Map(ID -> 2, VERSION -> "23")
    val sfoMeta = Map(ID -> 3)

    val airportsRDD = sc.makeRDD(Seq((otpMeta, otp), (mucMeta, muc), (sfoMeta, sfo)))
    airportsRDD.saveToEsWithMeta("airports/2015")

    sc.stop()
  }

}
