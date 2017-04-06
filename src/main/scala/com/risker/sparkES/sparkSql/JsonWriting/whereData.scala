package com.risker.sparkES.sparkSql.JsonWriting

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hc-3450 on 2016/11/23.
  */
object whereData {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("simplyFirst").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val options = Map("pushdown" -> "true", "es.nodes" -> "192.168.20.128", "es.port" -> "9200")
    val people = sqlContext.read.format("es").options(options).load("spark/people")
    people.printSchema()
//    root
//    |-- age: long (nullable = true)
//    |-- name: string (nullable = true)
    val filter = people.filter(people("name").equalTo("Michael").and(people("age").gt(21)))
    filter.foreach(println) //[29,Michael]
  }

}
