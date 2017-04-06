package com.risker.sparkSql.baseTest.Datasets

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hc-3450 on 2016/10/24.
  */
object FirstDatasets {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("fristfDataSets").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val ds = Seq(1, 2, 3).toDS()
    val rp1 = ds.map(_ + 1).collect()
    println(rp1.toBuffer) //ArrayBuffer(2, 3, 4)

    val ds2 = Seq(Person("Andy", 32)).toDS()
    val rp2 = ds2.map(_.name).collect()
    println(rp2.toBuffer)//ArrayBuffer(Andy)

    val path = "E:\\git\\spark-test\\src\\main\\scala\\resources\\people.json"
    val people = sqlContext.read.json(path).as[Person] //这样解析是不正确的
    val rp3 = people.map(_.age).collect()
    println(rp3.toBuffer)

    sc.stop()
  }
}

case class Person(name: String, age: Long)//要放到类外面