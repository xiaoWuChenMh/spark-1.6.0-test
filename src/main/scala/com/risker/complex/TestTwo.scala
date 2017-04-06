package com.risker.complex

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hui on 2016/6/3.
  */
object TestTwo {
  def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("testTwo").setMaster("local[2]")
      val source = Array("a","b","c","a","b","d","e","b")
      val sc = new SparkContext(conf)
      val data = sc.parallelize(source)
//      val rs = data.map((_,1)).reduceByKey(_+_).sortBy(x=>{
//        wordSort(x._1,x._2)
//      },true).collect()
     val rs = data.map((_,1)).reduceByKey(_+_).sortBy(_._2,false,5).collect()
     println(rs.toBuffer)
     sc.stop()
  }

  /***
    * 对两个参数都是数字的排序方法，通过sortBy来调用
    *
    * @param first
    * @param second
    */
  case class wordSort(first: Int,second: Int) extends Ordered[wordSort] with Serializable{
  override def compare(that: wordSort): Int = {
    if(this.second==that.second){
       this.first-that.first
    }else{
       that.second -this.second
      }
  }
}
}
