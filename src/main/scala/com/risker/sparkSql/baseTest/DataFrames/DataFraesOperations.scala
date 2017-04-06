package com.risker.sparkSql.baseTest.DataFrames

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by hc-3450 on 2016/10/24.
  */
object DataFraesOperations {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("firstDataFraes").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.read.json("E:\\git\\spark-test\\src\\main\\scala\\resources\\people.json")

    //显示DataFrame的内容
    data.show()
    //  +----+-------+
    //  | age|   name|
    //  +----+-------+
    //  |null|Michael|
    //  |  30|   Andy|
    //  |  19| Justin|
    //  +----+-------+

    //以树形打印DataFrame的表结构
    data.printSchema()
//    root
//    |-- age: long (nullable = true)
//    |-- name: string (nullable = true)

    //只显示name字段
    data.select("name").show()
//    +-------+
//    |   name|
//    +-------+
//    |Michael|
//    |   Andy|
//    | Justin|
//    +-------+

    //显示每条记录，并且年龄加1
    data.select(data("name"),data("age")+1).show()
//      +-------+---------+
//      |   name|(age + 1)|
//      +-------+---------+
//      |Michael|     null|
//      |   Andy|       31|
//      | Justin|       20|
//      +-------+---------+

    //显示年龄大于21的数据
    data.filter(data("age")>21).show()
//      +---+----+
//      |age|name|
//      +---+----+
//      | 30|Andy|
//      +---+----+

    //汇总相同年龄的人数 select age ,count(1) form table group by age
    data.groupBy("age").count().show()
//      +----+-----+
//      | age|count|
//      +----+-----+
//      |null|    1|
//      |  19|    1|
//      |  30|    1|
//      +----+-----+

    sc.stop()
  }
}
