package com.risker.spark.pit

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 共享变量——累加器 Accumulators
  *     累加器是一种只能通过关联操作进行“加”操作的变量，因此可以高效被并行支持。它们可以用来
  * 实现计数器（如MapReduce中）和求和器。Spark原生就支持Int和Double类型的累加器，开发者可以自己
  * 添加新的支持类型。如果被创建的累加器有名字，那可以在spakerUI中显示出来。这可以让我们了解当
  * 前执行的stages的情况。
  * Created by hc-3450 on 2016/9/7.
  */
object accumulatorTest {

  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("accumulatorTest")
                      .setMaster("local[2]")
      val sc = new SparkContext(conf)
      val accum = sc.accumulator(0)
      val data = sc.parallelize(1 to 10)
      data.map(x=>accum += x).collect()
      data.map(x=>accum += x).collect()
       println(accum.value)
      sc.stop()
  }
}
