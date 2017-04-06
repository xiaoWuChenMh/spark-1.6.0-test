package com.risker.spark.Transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  *      sortByKey作用于key-value形式的dataset，并对key来进行排序，它主要接受
  * 两个参数，第一个参数是Boolean,true代表升序，false代表降序，默认为true;第二
  * 个参数决定排序后的RDD的分区个数，默认和排序之前的分区个数相等。
  *  注意：
  *     1、该函数必须进行shuffle操作，因此结果RDD必然是ShufflleRDD
  *     2、只有调用collect或save 方法，RDD才会返回或输出有序的记录列表
  * Created by hc-3450 on 2016/9/5.
  */
object sortByKeyTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sortByKeyTest")
                    .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List((1,"a"),(3,"c"),(2,"b"),(4,"d")))
    val rs = data.sortByKey().collect()
    println(rs.toBuffer)
    sc.stop()
  }

}
