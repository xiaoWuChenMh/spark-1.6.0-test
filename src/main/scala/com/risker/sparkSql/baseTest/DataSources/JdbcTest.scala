package com.risker.sparkSql.baseTest.DataSources

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hc-3450 on 2016/10/26.
  */
object JdbcTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hiveTable").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
      val jdbcDF = sqlContext.read.format("jdbc").options(
        Map("url" -> "jdbc:mysql://192.168.20.126:3306/tpds",
          "driver" -> "com.mysql.jdbc.Driver",
          "dbtable" -> "rim_spark_table_rule",
          "user" -> "root",
          "password" -> "root"
        )).load()

    jdbcDF.printSchema() //输出 Schema
    jdbcDF.registerTempTable("rim_spark_table_rule") //注册为临时表
    sqlContext.cacheTable("rim_spark_table_rule")  //把指定的表缓存到内存中
    sqlContext.sql("select * from rim_spark_table_rule").map(x=>{
      x.getAs[String]("vendorCode")
    }).foreach(println)
    sc.stop()
  }

}
