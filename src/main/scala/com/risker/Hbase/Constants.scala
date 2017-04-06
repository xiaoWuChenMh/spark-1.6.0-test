package com.risker.Hbase

/**
  * Created by hc-3450 on 2017/4/1.
  */
object Constants {
  //########开发环境#########
  //  val ZK_CLUSTER="10.100.22.101,10.100.22.104,10.100.22.105"
  //  val FILE= "/src/main/resources/develop"
  //########生产环境#########
  val ZK_CLUSTER="10.100.22.36"
  //########测试环境#########
  //val ZK_CLUSTER="rmhadoop01:2181,rmhadoop04:2181,rmhadoop05:2181"
  //服务器配置文件存放地址
  val FILE= "/product"
  val ZK_PORT="2181"
  val HDFS_PATH="/NS1/ods/risk/%s/o_%s_s"
  val DEL_PATH="/NS1/ods/risk/%s"
  val DATE_FOR="yy/MM/dd"
  val DAYN = -10
}
