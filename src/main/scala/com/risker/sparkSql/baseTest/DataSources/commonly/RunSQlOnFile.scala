package com.risker.sparkSql.baseTest.DataSources.commonly

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在文件上执行执行sql查询
  * Created by hc-3450 on 2016/10/25.
  */
object RunSQlOnFile {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataSources2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //val data = sqlContext.sql("SELECT * FROM json.`E:\\git\\spark-test\\src\\main\\scala\\resources\\people.json`")
    //  data.show()
    val data = sqlContext.sql("SELECT * FROM parquet.`E:\\git\\spark-test\\src\\main\\scala\\resources\\users.parquet`")
    data.select("name", "favorite_color").collect().foreach(println)
    sc.stop()
  }
}
