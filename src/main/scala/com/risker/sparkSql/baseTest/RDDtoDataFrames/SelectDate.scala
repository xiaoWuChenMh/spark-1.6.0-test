package com.risker.sparkSql.baseTest.RDDtoDataFrames

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用反射获取Schema
  * 查询数据，实现步骤
  * 1、new一个 sqlContext 的实例
  * 2、导入数据
  * 3、创建 case class，相当于创建了一张表，有部分可以放在类外面或放在其他类里，不过有的必须在类中
  * 4、将RDD和case class关联
  * 5、将RDD转换成DataFrame（导入隐式转换，如果导入将无法把RDD转换成DataFrame）
  * 6、注册表
  * 7、输入select查询语句，这种方式传入的是 sql语句
  * 8、处理结果输出
  *   将结果以JSon的方式存储到指定位置
  *   在driver中打印输出查询结果
  *  9、对查询结果进行处理
  */
object SelectDate {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("selectDate")
      .setMaster("local[2]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    //1、new一个 sqlContext 的实例
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val source = List(("wMing","18","China"),("lHong","19","China")
                     ,("qYu","17","China"),("lPang","20","China"))

    //2、导入数据
    val data = sc.parallelize(source)

    //4、将RDD和case class关联
    val personRDD = data.map(t=>Person(t._1,t._2,t._3))

    //5、将RDD转换成DataFrame（导入隐式转换，如果导入将无法把RDD转换成DataFrame）
    import sqlContext.implicits._
    val personDF = personRDD.toDF()

    //6、注册表
    personDF.registerTempTable("person")

    //7、输入select查询语句，这种方式传入的是 sql语句
    val info = sqlContext.sql("select * from person order by age desc")

    //8、处理结果输出——将结果以JSon的方式存储到指定位置
   // info.write.json("F:\\home\\person.json")

    //8、处理结果输出——在driver中打印输出查询结果
    println(info.collect().toBuffer)
      //ArrayBuffer([lPang,20,China], [lHong,19,China], [wMing,18,China], [qYu,17,China])

    //9、对查询结果进行操作：根据下标筛选出所有的name字段
    info.map(t => "Name: " + t(0)).collect().foreach(println)
      //    Name: lPang
      //    Name: lHong
      //    Name: wMing
      //    Name: qYu
    //9、对查询结果进行操作：根据字段筛选出所有的name字段
    info.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)
      //    Name: lPang
      //    Name: lHong
      //    Name: wMing
      //    Name: qYu
    //9、对查询结果进行操作：按照规定的格式对数据进行整理
    info.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
      //    Map(name -> lPang, age -> 20)
      //    Map(name -> lHong, age -> 19)
      //    Map(name -> wMing, age -> 18)
      //    Map(name -> qYu, age -> 17)
  }

}
//3、创建case class,一定要放到外面
case class  Person(name:String,age:String,adree:String)