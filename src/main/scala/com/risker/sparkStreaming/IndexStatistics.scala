package com.risker.sparkStreaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object IndexStatistics{
  val split_column = "\001"
  val topic = "canal"
  val zkQuorum = "rmhadoop01:2181"
  val numThreads = "5"

  def main(args: Array[String]): Unit = {
    val confspark = new SparkConf().setAppName("IndexStatistics").setMaster("local[2]")
    val ssc = new StreamingContext(confspark, Seconds(2))
    ssc.checkpoint("E:\\work\\checkpoint")

    val topics = Set("canal")
    //本地虚拟机ZK地址
    val brokers = "rmhadoop01:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
     "serializer.class" -> "kafka.serializer.StringEncoder"
    )

    // Create a direct stream
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


    val resut_lines = lines.flatMap(line => {
      val key = line._1
      val name = line._2.split("\t")(0)
      val click = line._2.split("\t")(1).toLong
      System.out.println(click+" : "+name)
      Some(name,click)

    })




    val tmp = resut_lines.updateStateByKey(updateRunningSum _)

    tmp.foreachRDD(rdd=>{
      rdd.foreach(tuple=>{
        System.out.print(tuple._1+" : "+tuple._2)
      })
    })


    //resut_lines.print()
/*    val res3 = resut_lines.foreachRDD(rdd =>{
      rdd.foreachPartition(rdd_partition =>{
        rdd_partition.foreach(data=>{
          if(!data.toString.isEmpty) {
            DataUtil.toMySQL(data._1.toString,data._2.toInt)
          }
        })
      })
    })*/

    ssc.start()
    ssc.awaitTermination()
  }


  //将数据插入到数据库
  case class info(info1: String, info2: Int)

  def updateRunningSum(values: Seq[Long], state: Option[Long]) = {
    Some(state.getOrElse(0L)+values.sum)
  }

}
