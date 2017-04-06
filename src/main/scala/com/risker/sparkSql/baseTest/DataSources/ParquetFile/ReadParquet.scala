package com.risker.sparkSql.baseTest.DataSources.ParquetFile

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}



/**
  * 读取Parquet文件的例子，使用函数sqlContext.read.parquet()去读取文件
  * Created by hui on 2016/8/12.
  */
object ReadParquet {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ReadParquet").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    val people = sc.textFile("E:\\git\\spark-test\\src\\main\\scala\\resources\\people.txt").map(_.split(","))
      .map(p => Person(p(0), p(1).trim.toInt)).toDF()

    //数据写出为parquet文件
    people.write.parquet("E:\\git\\spark-test\\src\\main\\scala\\resources\\people.parquet")
    //然后读取parquet文件
    val parquetFile = sqlContext.read.parquet("E:\\git\\spark-test\\src\\main\\scala\\resources\\people.parquet")
    //注册为表parquetFile
    parquetFile.registerTempTable("parquetFile")
    //sql查询
    val teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
    //输出结果
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
    //Name: Justin  只有它满足条件
  }

}

case class Person(name: String, age: Int)