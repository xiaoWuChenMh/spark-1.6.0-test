package com.risker.Hbase

import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, ResultScanner, Scan}

/**
  * 参考：http://www.ithao123.cn/content-10468080.html
  * Created by hc-3450 on 2017/4/1.
  */
object HbaseClientMy {

  val LINES_BATCH = 1000000

  def main(args: Array[String]): Unit = {
    connectHbase2Scan(args)
  }

  /**
    * 获取hbase数据
    */
  def connectHbase2Scan(hTables:Array[String]): Unit = {
    val conf: Configuration = HBaseConfiguration.create
    conf.set("hbase.zookeeper.quorum", Constants.ZK_CLUSTER)
    conf.set("hbase.zookeeper.property.clientPort", Constants.ZK_PORT)
    var table: HTable = null
    val scan: Scan = new Scan
    var scanner: ResultScanner = null
    val buf = new StringBuilder()
    for(l <-0 until  hTables.length){
      try {
        table = new HTable(conf, hTables(l))
        scanner = table.getScanner(scan)
        var treasureMap = Map[Int, String]()
        var rowKey = ""
        var count = 0
        //        for (r <- scanner) {
        //          count += 1
        //        }
        //        println("Htable:"+hTables(l)+"-----"+"count:"+count)
        scanner.close
      }
      catch {
        case e: Exception =>
      } finally {
        scanner.close
      }
    }
  }

  /**
    * 返回按照字段排序的每行数据
    *
    * @param map
    * @return
    */
  def mapSort(map: Map[Int, String], rowKey: String): String = {
    val buf = new StringBuilder()
    val sysTime = Calendar.getInstance().getTimeInMillis()
    map.toList.sorted foreach {
      case (key, value) =>
        if (value == "") buf.append("\\N") else buf.append(value + "\001")

    }
    rowKey + "\001" + buf.toString() + sysTime + "\n"
  }

}
