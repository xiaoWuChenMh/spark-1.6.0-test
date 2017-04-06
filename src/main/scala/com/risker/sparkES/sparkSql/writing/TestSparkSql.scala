package com.risker.sparkES.sparkSql.writing

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._

/**
  * Created by hc-3450 on 2016/11/28.
  */
object TestSparkSql {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("simplyFirst")
    sparkConf.set("es.nodes","192.168.20.128");
    sparkConf.set("es.port","9200");
    sparkConf.set("es.index.auto.create", "true");
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val pesons =List("马辉,北京市海淀","mahui,河北省保定市")

    import sqlContext.implicits._
    val people = pesons.map(_.split(","))
      .map(p => Person1(p(0), p(1)))
      .toDF()
     people.saveToEs("spark/people")

    sc.stop()

  }

}
case class Person1(name: String, adrre: String)