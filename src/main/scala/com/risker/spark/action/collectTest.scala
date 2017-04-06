package com.risker.spark.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * action——collect的使用
  * 计算是放在Executor的内存中做的，同样结果也保存在内存中，
  * 我们在driver如果想要得到计算结果就需要使用这个算子，
  * 它会以数组的形式返回数据集的所有元素。
  */
object collectTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("collectTest").setMaster("local[2]")

    val sc =new SparkContext(conf)

    //读取文件
    val data = sc.textFile("F:\\home\\action\\collect.txt",3)

    //计算单词出现的个数
    val result = data.flatMap(line=>line.split(" ")).map(a=>(a,1)).reduceByKey((a,b)=>a+b)
    //val result = data.flatMap(line=>line.split(" ")).map(a=>(a,1)).reduceByKey(_+_)

    //通过collect，收集处理结果到driver端
    val show = result.collect()

    //收集的结果集是数组，所以调用 toBuffer 方法
    println(show.toBuffer)

    sc.stop()

  }

}
