package com.risker.sparkSql.baseTest.DataSources.ParquetFile

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Schema合并
  * Created by hc-3450 on 2016/10/26.
  */
object SchemaMerging {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("schemaMerging").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
    df1.write.parquet("E:\\git\\spark-test\\src\\main\\scala\\resources\\data\\test_table\\key=1")
    //      root
    //      |-- single: integer (nullable = false)
    //      |-- double: integer (nullable = false)

    val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
    df2.write.parquet("E:\\git\\spark-test\\src\\main\\scala\\resources\\data\\test_table\\key=2")
    //    root
    //    |-- single: integer (nullable = false)
    //    |-- triple: integer (nullable = false)
    val df3 = sqlContext.read.option("mergeSchema", "true").parquet("E:\\git\\spark-test\\src\\main\\scala\\resources\\data\\test_table")
    df3.printSchema()
    //    root
    //    |-- single: integer (nullable = true)
    //    |-- double: integer (nullable = true)
    //    |-- triple: integer (nullable = true)
    //    |-- key: integer (nullable = true)
    sc.stop()
  }

}
