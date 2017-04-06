package com.risker.sparkES.sparkCore.Writing

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._ //隐式转换

/**
  *  数据写入到不同的 type中（桶中）
  * Created by hc-3450 on 2016/11/22.
  */
object dynamicWriting {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("EsTest").setMaster("local[2]")
    //配置 es的节点信息
    sparkConf.set("es.nodes","192.168.20.128:9200");
    //elasticsearch-hadoop是否应该创建一个索引(如果索引失踪)当写入数据到Elasticsearch里或写失败时。
    sparkConf.set("es.index.auto.create", "true");
    val sc = new SparkContext(sparkConf)

    val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")
    sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection/{media_type}")
  }

}
