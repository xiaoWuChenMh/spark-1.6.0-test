package com.risker.sparkStreaming

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * updateStateByKey 解释:
  *   以DStream中的数据进行按key做reduce操作，然后对各个批次的数据进行累加在有新的数据信息进入或更新时，
  *   可以让用户保持想要的任何状。使用这个功能需要完成两步：
  *      1) 定义状态：可以是任意数据类型
  *      2) 定义状态更新函数：用一个函数指定如何使用先前的状态，从输入流中的新值更新状态。
  *   对于有状态操作，要不断的把当前和历史的时间切片的RDD累加计算，随着时间的流失，计算的数据规模会变得越来越大。
  * Created by hc-3450 on 2016/11/24.
  */
object UserUpdateStateByKey {

  val topics = Set("mhTest")

  val brokers = "rmhadoop01:9092"

  val kafkaParams = Map[String, String](
    "metadata.broker.list" -> brokers,
    "group.id" -> "mh"
  )

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("UserUpdateStateByKey").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(3))
    ssc.checkpoint("E:\\work\\checkpoint")

    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


    val resut_lines = lines.flatMap(line => {
      val value = line._2.split(",")
      System.out.println(" : "+value(0))
      Some(value(0),value(1).toLong)
    })

    val tmp = resut_lines.updateStateByKey(updateFunc _)

    tmp.foreachRDD(rdd=>{
      rdd.foreach(tuple=>{
        System.out.print(tuple._1+" : "+tuple._2)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

  def updateFunc(values: Seq[Long], state: Option[Long]) = {
    //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
    val currentCount = values.sum
    // 已累加的值,历史
    val previousCount = state.getOrElse(0L)
    // 返回累加后的结果，是一个Option[Int]类型
    Some(currentCount + previousCount)
  }


}
