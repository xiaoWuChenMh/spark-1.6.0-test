package com.risker.spark.pit

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 共享变量——广播变量 Broadcast Variables
  *      广播变量允许程序员保留一个只读的变量，缓存在每一台机器上，而非每个任务保存一份拷贝。
  * 他们可以这样被使用，例如，以一种高效的方式给每个结点一个大的输入数据集。Spark会尝试使用
  * 一种高效的广播算法来传播广播变量，从而减少通信的代价。
  * Created by hc-3450 on 2016/10/21.
  */
object Broadcast {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Broadcast").setMaster("local[3]")
    val sc = new SparkContext(sparkConf)
    //TODO 创建一个广播变量 broadcastVar
    val broadcastVar = sc.broadcast(("王璐璐","程明明"))
    val data = sc.parallelize(1 to 100).cache()
    data.map(x=>{
      x +":"+broadcastVar.value._2+"||"  //TODO 通过value函数获取广播变量的值
    }).top(12).foreach(println)

  }
}
