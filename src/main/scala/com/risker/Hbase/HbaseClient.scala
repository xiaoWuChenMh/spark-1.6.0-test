//package com.credithc.test
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.{HTable, ResultScanner, Scan}
//import org.joda.time.DateTime
//
//import scala.io.Source
//
///**
//  * Created by hc-3450 on 2016/11/4.
//  */
//object HbaseClient {
//
//  val LINES_BATCH = 1000000
//
//  /**
//    * 获取hbase数据
//    */
//  def connectHbase2Scan: Unit = {
//    val timeDir = new DateTime().plusDays(-1).toString(Constants.DATE_FOR)
//    //N天前
//    val dtBeforeN = new DateTime().plusDays(Constants.DAYN).toString(Constants.DATE_FOR)
//    val conf: Configuration = HBaseConfiguration.create
//    conf.set("hbase.zookeeper.quorum", Constants.ZK_CLUSTER)
//    conf.set("hbase.zookeeper.property.clientPort", Constants.ZK_PORT)
//    var table: HTable = null
//    val scan: Scan = new Scan
//    var scanner: ResultScanner = null
//    var hTable = ""
//
//    //读取配置文件获取Hbase表
//    for (line <- Source.fromFile(System.getProperty("user.dir") + Constants.FILE).getLines) {
//      hTable = line.split(" ")(0)
//      val buf = new StringBuilder()
//      try {
//        val path = Constants.HDFS_PATH.format(timeDir, hTable)
//        table = new HTable(conf, hTable)
//        scanner = table.getScanner(scan)
//        import scala.collection.JavaConversions._
//        var treasureMap = Map[Int, String]()
//        var rowKey = ""
//        var count = 0
//        for (r <- scanner) {
//          count += 1
//          var str = ""
//          for (keyValue <- r.raw) {
//            rowKey = new String(keyValue.getRow)
//            treasureMap += (Integer.parseInt(new String(keyValue.getQualifier)) -> new String(keyValue.getValue))
//          }
//          buf.append(mapSort(treasureMap, rowKey))
//          if (buf.size >= LINES_BATCH) {
//            Save2HdfsA.save2HdfsA(buf.toString(), path)
//            buf.clear()
//          }
//        }
//        logWarning("Htable:"+hTable+"-----"+"count:"+count)
//        //数据保存至hdfs
//        Save2HdfsA.save2HdfsA(buf.toString(), path)
//        //删除前N天的数
//        DeleteHdfsDir.deleteDir(Constants.DEL_PATH.format(dtBeforeN))
//        scanner.close
//      }
//      catch {
//        case e: Exception => logError(s" ###########Hbase read this table error:$hTable the error message is" + e.printStackTrace() + "#############")
//      } finally {
//        scanner.close
//      }
//    }
//  }
//
//  /**
//    * 返回按照字段排序的每行数据
//    *
//    * @param map
//    * @return
//    */
//  def mapSort(map: Map[Int, String], rowKey: String): String = {
//    val buf = new StringBuilder()
//    val sysTime = Calendar.getInstance().getTimeInMillis()
//    map.toList.sorted foreach {
//      case (key, value) =>
//        if (value == "") buf.append("\\N") else buf.append(value + "\001")
//
//    }
//    rowKey + "\001" + buf.toString() + sysTime + "\n"
//  }
//
//}
