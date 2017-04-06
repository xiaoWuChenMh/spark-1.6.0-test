package com.risker.sparkES.sparkStreaming.writing

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.streaming._

import scala.collection.mutable

/**
  * 把现有的json写入到es
  * Created by hc-3450 on 2016/11/24.
  */
object wJson {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wDstream").setMaster("local[2]")
    conf.set("es.nodes","192.168.20.128");
    conf.set("es.port","9200");
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))

    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""
    val rdd = sc.makeRDD(Seq(json1, json2))
    val microbatch = mutable.Queue(rdd)
    ssc.queueStream(microbatch).saveJsonToEs("spark/json-trips")
    ssc.start()
    ssc.awaitTermination()
  }

}
