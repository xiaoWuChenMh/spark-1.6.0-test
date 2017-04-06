package com.risker.sparkES.sparkSql.JsonWriting

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过命令读取es中的数据
  * Created by hc-3450 on 2016/11/23.
  */
object JosnData {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("simplyFirst").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val options = Map("pushdown" -> "true", "es.nodes" -> "192.168.20.128", "es.port" -> "9200")
    val spark14DF = sqlContext.read.format("es").options(options).load("spark/people")
    spark14DF.printSchema()
//    root
//    |-- age: long (nullable = true)
//    |-- name: string (nullable = true)
    spark14DF.registerTempTable("people")
    sqlContext.sql("select * from people").foreach(println)
//      [19,Justin]
//      [29,Michael]
//      [30,Andy]
  }


}
