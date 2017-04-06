package com.risker.sparkSql.baseTest.DataSources

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * josn字符串转换为RDD后在转换为DataFrame
  * Created by hc-3450 on 2017/2/17.
  */
object JsonString {

  def main(args: Array[String]): Unit = {
    var source = "[{\"insuranceType\":\"基本养老保险\",\"payTime\":\"2017-01-05\",\"payBase\":2017,\"selfPayAmount\":161.36,\"selfPayPercent\":8,\"compPayAmount\":383.23,\"compPayPercent\":19,\"incomeAmount\":0,\"compName\":\"四川省远景劳动和社会保障代理有限责任公司\",\"payStatus\":\"参保缴费\",\"actualTime\":\"2017-01-05\"},{\"insuranceType\":\"基本养老保险\",\"payTime\":\"2016-12-02\",\"payBase\":2017,\"selfPayAmount\":161.36,\"selfPayPercent\":8,\"compPayAmount\":383.23,\"compPayPercent\":19,\"incomeAmount\":0,\"compName\":\"四川省远景劳动和社会保障代理有限责任公司\",\"payStatus\":\"参保缴费\",\"actualTime\":\"2016-12-02\"},{\"insuranceType\":\"失业保险\",\"payTime\":\"2016-09-02\",\"payBase\":2874,\"selfPayAmount\":11.5,\"selfPayPercent\":0.4,\"compPayAmount\":17.24,\"compPayPercent\":0.6,\"incomeAmount\":0,\"compName\":\"四川省远景劳动和社会保障代理有限责任公司\",\"payStatus\":\"参保缴费\",\"actualTime\":\"2016-09-02\"},{\"insuranceType\":\"失业保险\",\"payTime\":\"2016-08-01\",\"payBase\":2874,\"selfPayAmount\":11.5,\"selfPayPercent\":0.4,\"compPayAmount\":17.24,\"compPayPercent\":0.6,\"incomeAmount\":0,\"compName\":\"四川省远景劳动和社会保障代理有限责任公司\",\"payStatus\":\"参保缴费\",\"actualTime\":\"2016-08-01\"},{\"insuranceType\":\"基本医疗保险\",\"payTime\":\"2016-10-09\",\"payBase\":2874,\"selfPayAmount\":57.48,\"selfPayPercent\":2,\"compPayAmount\":186.81,\"compPayPercent\":6.5,\"incomeAmount\":65.53,\"compName\":\"四川省远景劳动和社会保障代理有限责任公司\",\"payStatus\":\"参保缴费\",\"actualTime\":\"2016-10-09\"},{\"insuranceType\":\"基本医疗保险\",\"payTime\":\"2016-09-02\",\"payBase\":2874,\"selfPayAmount\":57.48,\"selfPayPercent\":2,\"compPayAmount\":186.81,\"compPayPercent\":6.5,\"incomeAmount\":65.53,\"compName\":\"四川省远景劳动和社会保障代理有限责任公司\",\"payStatus\":\"参保缴费\",\"actualTime\":\"2016-09-02\"},{\"insuranceType\":\"工伤保险\",\"payTime\":\"2016-11-01\",\"payBase\":2874,\"selfPayAmount\":0,\"selfPayPercent\":0,\"compPayAmount\":4.02,\"compPayPercent\":0.14,\"incomeAmount\":0,\"compName\":\"四川省远景劳动和社会保障代理有限责任公司\",\"payStatus\":\"参保缴费\",\"actualTime\":\"2016-11-01\"},{\"insuranceType\":\"工伤保险\",\"payTime\":\"2016-10-09\",\"payBase\":2874,\"selfPayAmount\":0,\"selfPayPercent\":0,\"compPayAmount\":4.02,\"compPayPercent\":0.14,\"incomeAmount\":0,\"compName\":\"四川省远景劳动和社会保障代理有限责任公司\",\"payStatus\":\"参保缴费\",\"actualTime\":\"2016-10-09\"},{\"insuranceType\":\"生育保险\",\"payTime\":\"2015-12-30\",\"payBase\":2584,\"selfPayAmount\":0,\"selfPayPercent\":0,\"compPayAmount\":12.92,\"compPayPercent\":0.5,\"incomeAmount\":0,\"compName\":\"四川省远景劳动和社会保障代理有限责任公司\",\"payStatus\":\"参保缴费\",\"actualTime\":\"2015-12-30\"},{\"insuranceType\":\"生育保险\",\"payTime\":\"2015-12-01\",\"payBase\":2584,\"selfPayAmount\":0,\"selfPayPercent\":0,\"compPayAmount\":12.92,\"compPayPercent\":0.5,\"incomeAmount\":0,\"compName\":\"四川省远景劳动和社会保障代理有限责任公司\",\"payStatus\":\"参保缴费\",\"actualTime\":\"2015-12-01\"},{\"insuranceType\":\"大病医疗补充保险\",\"payTime\":\"2016-10-09\",\"payBase\":2874,\"selfPayAmount\":0,\"selfPayPercent\":0,\"compPayAmount\":28.74,\"compPayPercent\":1,\"incomeAmount\":0,\"compName\":\"四川省远景劳动和社会保障代理有限责任公司\",\"payStatus\":\"参保缴费\",\"actualTime\":\"2016-10-09\"},{\"insuranceType\":\"大病医疗补充保险\",\"payTime\":\"2016-09-02\",\"payBase\":2874,\"selfPayAmount\":0,\"selfPayPercent\":0,\"compPayAmount\":28.74,\"compPayPercent\":1,\"incomeAmount\":0,\"compName\":\"四川省远景劳动和社会保障代理有限责任公司\",\"payStatus\":\"参保缴费\",\"actualTime\":\"2016-09-02\"}]"
    val conf = new SparkConf().setMaster("local[2]").setAppName("Test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val tesRDD = sc.makeRDD(source::Nil)
    val tableName = sqlContext.read.json(tesRDD)
    tableName.printSchema()
    tableName.registerTempTable("people")
    val re1 = sqlContext.sql("select insuranceType,sum(selfPayAmount) from people  group by insuranceType")
    re1.collect().foreach(x=>{
      println(x)
    })
    sc.stop()

  }

}
