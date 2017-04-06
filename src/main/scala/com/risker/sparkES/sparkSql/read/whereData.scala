package com.risker.sparkES.sparkSql.read

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._

/**
  * es复杂查询语句
  * Created by hc-3450 on 2016/11/23.
  */
object whereData {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("simplyFirst").setMaster("local[2]")
    sparkConf.set("es.nodes","192.168.20.128");
    sparkConf.set("es.port","9200");
    sparkConf.set("es.index.auto.create", "true");
//    sparkConf.set("es.read.field.include", "*name"); //包含某些字段
//    sparkConf.set("es.read.field.exclude", "*name"); //排序某些字段
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val query = """ {"query": { "match_all": {} }} """ //可以匹配
    val query1 = """ {"query": { "match": {"age":19} }} """ //可以匹配
    val query2 = """ {"query": { "match_all": {} }, "from": 0, "size": 1 } """ //from ,size这种语法不支持
    sqlContext.esDF("spark/people",query).foreach(println)

  }

}
