package com.risker.sparkES.sparkSql.writing

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._

/**
  * Created by hc-3450 on 2016/11/23.
  */
object SimplyFirst {

  def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf().setAppName("simplyFirst").setMaster("local[2]")
      sparkConf.set("es.nodes","192.168.20.128");
      sparkConf.set("es.port","9200");
      sparkConf.set("es.index.auto.create", "true");
      val sc = new SparkContext(sparkConf)
      val sqlContext = new SQLContext(sc)

      import sqlContext.implicits._
      val people = sc.textFile("E:\\git\\spark-test\\src\\main\\scala\\resources\\people.txt")
        .map(_.split(","))
        .map(p => Person(p(0), p(1).trim.toInt))
        .toDF()
       people.saveToEs("spark/people")
  }

}
case class Person(name: String, adrre: Int)