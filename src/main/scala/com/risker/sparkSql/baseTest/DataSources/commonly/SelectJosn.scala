package com.risker.sparkSql.baseTest.DataSources.commonly

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  spark sql 处理 结构化json文本常用的一些操作
  *
  *  实例json的内容（表的字段是name 和 age）
  *      {"name":"Michael"}
  *      {"name":"Andy", "age":30}
  *      {"name":"Justin", "age":19}
  */
object SelectJosn {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("selectJson")
      .setMaster("local[2]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    //1、new一个 sqlContext 的实例
    val sqlContext = new SQLContext(sc)

    //2、创建一个DataFrame
    val data = sqlContext.read.json("F:\\home\\people.json")

    //显示表内容
    data.show()
    // age  |   name
    // null | Michael
    //   30 | Andy
    //   19 | Justin

    //显示数据表的结构数据
    data.printSchema()
    //root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    //只查询 name列
    data.select("name").show()
    // |   name  |
    // |Michael  |
    // |   Andy  |
    // | Justin  |

    //查询每个人的信息,但是年龄递增1
    data.select(data("name"),data("age")+1).show()
      //  |   name|(age + 1)|
      //  |Michael|     null|
      //  |   Andy|       31|
      //  | Justin|       20|

    //查询 年龄大于21 的人的信息
    data.filter(data("age")>21).show()
    // |age|name|
    // | 30|Andy|

    //根据年龄进行分组，并count个数
    data.groupBy(data("age")).count().show()
    //  | age|count|
    //  |null|    1|
    //  |  19|    1|
    //  |  30|    1|
    data.groupBy(data("age")).count().write.json("F:\\home\\person.json")
  }


}
