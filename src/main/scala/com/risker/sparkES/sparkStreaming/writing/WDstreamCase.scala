package com.risker.sparkES.sparkStreaming.writing

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable
import org.elasticsearch.spark.streaming._

/**
  * 使用 样例类
  * Created by hc-3450 on 2016/11/24.
  */
object WDstreamCase {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wDstream").setMaster("local[2]")
    conf.set("es.nodes","192.168.20.128");
    conf.set("es.port","9200");
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))

    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")
    val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
    val microbatches = mutable.Queue(rdd)
    val dstream = ssc.queueStream(microbatches)
    EsSparkStreaming.saveToEs(dstream, "spark/docs")
    ssc.start()
    ssc.awaitTermination()
  }

}
case class Trip(departure: String, arrival: String)