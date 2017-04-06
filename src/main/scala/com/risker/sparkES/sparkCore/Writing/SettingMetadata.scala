package com.risker.sparkES.sparkCore.Writing

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

/**
  * 简单的设置一个元数据的配置(这种方式只能设置id)
  * Created by hc-3450 on 2016/11/22.
  */
object SettingMetadata {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("EsTest").setMaster("local[2]")
    sparkConf.set("es.nodes","rmhadoop01,rmhadoop02,rmhadoop03,rmhadoop04,rmhadoop05");
    sparkConf.set("es.port","9200");
    sparkConf.set("es.index.auto.create", "true");
    val sc = new SparkContext(sparkConf)
    val otp = Map("iata" -> "中航4", "name" -> "Otopeni")
      val muc = Map("iata" -> "南航", "name" -> "Munich")
    val sfo = Map("iata" -> "北航", "name" -> "San Fran")
    var list = new ListBuffer[Tuple2[Int,immutable.Map[String,String]]]()
    list.append((3, otp))
    val airportsRDD = sc.makeRDD(list)
//    val airportsRDD = sc.makeRDD(ListBuffer((1, otp), (2, muc), (3, sfo)))
    airportsRDD.saveToEsWithMeta("airports/2015")
    sc.stop()
  }

}
