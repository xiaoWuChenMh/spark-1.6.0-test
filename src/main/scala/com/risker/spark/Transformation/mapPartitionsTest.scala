package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hc-3450 on 2016/9/2.
  * 和map很像，但RDD是分布式的运行在每一个分区上。因此func运行在一个RDD
  * 上时类型必须是 Iterator<T> => Iterator<U>
  *
  */
object mapPartitionsTest {

  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("mapPartitonsTest")
                .setAppName("local[2]")
     val sc = new SparkContext(conf)
     val data = sc.textFile("hdfs://test/file1")
     data.mapPartitions(partitions=>{
       //每一个分区中的每一个元素都加1
       partitions.map(_+1)
     })
    sc.stop()
  }
}
