package com.risker.Test

import org.apache.spark.util.Utils
import parquet.schema.Types.ListBuilder

import scala.collection.mutable.ListBuffer

/**
  * Created by hc-3450 on 2016/11/8.
  */
class Test(){

  def vile(): Unit ={
    val sd = this.getClass.getSimpleName.replace("$", "")
    println(sd)
  }

}
object Test {

    def main(args: Array[String]): Unit = {
      new Test().vile
  }


}
