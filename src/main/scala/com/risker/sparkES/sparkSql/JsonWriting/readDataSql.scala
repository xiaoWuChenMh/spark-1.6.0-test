package com.risker.sparkES.sparkSql.JsonWriting

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过sql读取es中的数据
  * Created by hc-3450 on 2016/11/23.
  */
object readDataSql {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("simplyFirst").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    sqlContext.sql(
      "CREATE TEMPORARY TABLE myIndex    " +
        "USING org.elasticsearch.spark.sql " +  // "USING es "
        "OPTIONS ( resource 'spark/people', nodes '192.168.20.128',port '9200')" )

    sqlContext.sql("select * from myIndex").foreach(println)
//    [19,Justin]
//    [29,Michael]
//    [30,Andy]
  }

}
