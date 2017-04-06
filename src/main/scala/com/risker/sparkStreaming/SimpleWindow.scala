package com.risker.sparkStreaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hc-3450 on 2016/11/24.
  */
object SimpleWindow {

  val topics = Set("mhTest")

  val brokers = "rmhadoop01:9092"

  val kafkaParams = Map[String, String](
    "metadata.broker.list" -> brokers,
    "group.id" -> "mh"
  )

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("windowTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(3))

    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    lines.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        rdd.foreachPartition(part=>{
          if(!part.isEmpty){
            part.foreach(x=>{
              println("key:"+x._1+" || value:"+x._2)
              Some()
            })
          }
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
