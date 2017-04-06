package com.risker.sparkES.sparkCore.Writing

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * 创建一个新的index/type  spark/docs
  *
  * 使用的是隐式转换的方式 把数据导入 es
  * Created by hc-3450 on 2016/11/18.
  */
object sparkWritingEs {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("EsTest").setMaster("local[2]")
    //配置 es的节点信息
    sparkConf.set("es.nodes","192.168.20.128:9200");
    //elasticsearch-hadoop是否应该创建一个索引(如果索引失踪)当写入数据到Elasticsearch里或写失败时。
    sparkConf.set("es.index.auto.create", "true");
    val sc = new SparkContext(sparkConf)
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
    //makeRDD  根据指定的集合创建一个专用的RDD.任何RDD都可以放入到其内。
    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")
    //保存结果解释：index的内容是两个文档，保存到Elasticsearch里的 spark/docs 下。
    sc.stop()
  }

}
