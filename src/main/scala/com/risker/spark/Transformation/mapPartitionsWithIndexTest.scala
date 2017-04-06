package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 与mapPartitions相似相似,但它为函数提供了一个整数值代表分区的索引
  * 因此当在一个RDD上运行一个func的时候函数类型必须是 (Int, Iterator<T>) => Iterator<U>
  */
object mapPartitionsWithIndexTest {

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("mapPartitionsWithIndexTest")
                  .setMaster("local[3]")
    var sc = new SparkContext(conf)
    val data = sc.parallelize(List(1,2,9,3,6,7,4,6,8))
    data.mapPartitionsWithIndex(
      (i,iter)=>{
        iter.map(x=>(i,x+"  "))
      }).reduceByKey(_+_).foreach(println)
    sc.stop()
  }


}
