package com.risker.sparkES.sparkSql.read

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._


/**
  *
  * es中的数据进行表关联
  * Created by hc-3450 on 2016/11/23.
  */
object JoinTable {

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
    val people = sqlContext.esDF("spark/people",query)
    val adrre = sqlContext.esDF("spark/adrre",query)
    people.registerTempTable("people")
    adrre.registerTempTable("adrre")
    println("=================表关联========OK=========")
    val de =  sqlContext.sql("select * from people p left join adrre d " +
      " on p.name=d.name where p.age=19")
    de.foreach(println)

    sqlContext.sql("select * from people p where p.age=19").foreach(println)
  }

}
