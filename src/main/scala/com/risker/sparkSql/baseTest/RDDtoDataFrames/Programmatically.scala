package com.risker.sparkSql.baseTest.RDDtoDataFrames

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
//Import Row.
import org.apache.spark.sql.Row;
//Import Spark SQL data types
import org.apache.spark.sql.types.{StringType, StructField, StructType};

/**
  *以编程的方式指定模式
  */
object Programmatically {

  def main(args: Array[String]) {

    var conf = new SparkConf().setAppName("Programmatically")
        .setMaster("local[2]")
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    var sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    //创建一个RDD
    val people = sc.textFile("E:\\git\\spakr-mahui-test\\spark-test-1.6.0\\src\\main\\scala\\resources\\people.txt")

    //编码的模式是一个字符串
    val schemaString = "name age"

    //生成模式——基于字符串的模式
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // DD的记录(people) 转变为Row
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    //将模式应用于RDD 得到一个DataFrame
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    peopleDataFrame.show()

    //把DataFrames 转换为一个表
    peopleDataFrame.registerTempTable("people")

    // Sql语句可以被sqlContext提供的sql方法来运行
    val results = sqlContext.sql("SELECT name FROM people")

    //sql查询的结果是DataFrames并支持所有正常的抽样操作。
    //一行中的列可以在结果中通过字段索引或者字段名称访问
    results.map(t => "Name: " + t(0)).collect().foreach(println)

    sc.stop()
  }
}















