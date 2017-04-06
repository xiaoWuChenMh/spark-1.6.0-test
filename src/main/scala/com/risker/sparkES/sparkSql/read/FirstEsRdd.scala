package com.risker.sparkES.sparkSql.read

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._
/**
  * Created by hc-3450 on 2016/11/23.
  */
object FirstEsRdd {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("simplyFirst").setMaster("local[2]")
    sparkConf.set("es.nodes","192.168.20.128");
    sparkConf.set("es.port","9200");
    sparkConf.set("es.index.auto.create", "true");
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val people = sqlContext.esDF("spark/people")
    println(people.schema.treeString) //等同people.printSchema()
//    root
//    |-- age: long (nullable = true)
//    |-- name: string (nullable = true)
    people.foreach(println)
//      [19,Justin]
//      [29,Michael]
//      [30,Andy]



  }

}
