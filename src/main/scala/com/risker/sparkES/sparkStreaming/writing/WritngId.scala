package com.risker.sparkES.sparkStreaming.writing

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.streaming.EsSparkStreaming

import scala.collection.mutable

/**
  *
  * 指定id，但会有一个名为id的字段
  * Created by hc-3450 on 2016/11/24.
  */
object caseConf {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wDstream").setMaster("local[2]")
    conf.set("es.nodes","192.168.20.128");
    conf.set("es.port","9200");
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))

    val upcomingTrip = Trip1("aaaa", "1111","1")
    val lastWeekTrip = Trip1("qqqq", "2222","2")
    val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
    val microbatches = mutable.Queue(rdd)
    val dstream = ssc.queueStream(microbatches)
    EsSparkStreaming.saveToEs(dstream, "spark/docs",Map("es.mapping.id" -> "id"))
    ssc.start()
    ssc.awaitTermination()
  }


}
case class Trip1(departure: String, arrival: String,id:String)