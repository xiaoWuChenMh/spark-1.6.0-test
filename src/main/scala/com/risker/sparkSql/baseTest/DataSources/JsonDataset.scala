package com.risker.sparkSql.baseTest.DataSources

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hc-3450 on 2016/10/26.
  */
object JsonDataset {

  def main(args: Array[String]): Unit = {
     val sparkConf = new SparkConf().setAppName("jsonDataset")
     if(args.length==0) sparkConf.setMaster("local[2]")
     val sc = new SparkContext(sparkConf)
     val sparkSql = new SQLContext(sc)
     val path = "E:\\git\\spark-test\\src\\main\\scala\\resources\\people.json"
     val people = sparkSql.read.json(path)
     people.printSchema()
    //    root
    //    |-- age: long (nullable = true)
    //    |-- name: string (nullable = true)
    people.registerTempTable("people")
    val re1 = sparkSql.sql("select name from people where age>=13 and age<=19")
    re1.foreach(print) //[Justin]
    val anotherPeopleRDD = sc.parallelize(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val anotherPeople = sparkSql.read.json(anotherPeopleRDD)
    anotherPeople.registerTempTable("peopleTwo")
    val re2 = sparkSql.sql("select * from peopleTwo")
    re2.collect().foreach(println)//[[Columbus,Ohio],Yin]
    sc.stop()

  }


}
