package com.risker.sparkES.sparkStreaming.writing

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.streaming._
import scala.collection.mutable

/**
  * 数据类型需要是 map 然后放入到seq中
  * Created by hc-3450 on 2016/11/24.
  */
object wDstream {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wDstream").setMaster("local[2]")
    conf.set("es.nodes","192.168.20.128");
    conf.set("es.port","9200");
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
    val rdd = sc.makeRDD(Seq(numbers, airports))
    val microbatches = mutable.Queue(rdd)
    ssc.queueStream(microbatches).saveToEs("spark/docs")
    ssc.start()
    ssc.awaitTermination()
  }

}
