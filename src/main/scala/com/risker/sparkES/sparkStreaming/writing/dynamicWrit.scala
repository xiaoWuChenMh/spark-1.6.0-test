package com.risker.sparkES.sparkStreaming.writing

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.streaming._

import scala.collection.mutable

/**
  * 灵活的配置type名，但type的明也会成为一个字段
  * Created by hc-3450 on 2016/11/24.
  */
object dynamicWrit {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wDstream").setMaster("local[2]")
    conf.set("es.nodes","192.168.20.128");
    conf.set("es.port","9200");
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))

    val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")

    val batch = sc.makeRDD(Seq(game, book, cd))
    val microbatches = mutable.Queue(batch)
    ssc.queueStream(microbatches).saveToEs("my-collection/{media_type}")
    ssc.start()
    ssc.awaitTermination()
  }

}
