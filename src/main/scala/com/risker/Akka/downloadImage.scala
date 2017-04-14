package com.risker.Akka

import java.io.FileOutputStream
import java.net.URL

import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import org.jsoup.Jsoup

/**
  * 作用：从煎蛋下载图片
  * 来源：开源中国
  * 实现：通过使用Actor 实现多线程从html中获取图片下载到本地
  * jsoup 是一款Java 的HTML解析器，可直接解析某个URL地址、HTML文本内容。
  * 它提供了一套非常省力的API，可通过DOM，CSS以及类似于jQuery的操作方法来取出
  * 和操作数据。
  * Created by hc-3450 on 2017/4/14.
  */
object downloadImage {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("dowmn")
    val jiandan = system.actorOf(Props[JiandanActor],name="JiandanActor")
    jiandan!"start"
    Thread.sleep(5000)
    system.shutdown()
  }


  class JiandanActor extends Actor {
    val html = context.actorOf(Props[HtmlActor], "html")
    def receive = {
      case "start" => {
        html ! "http://jandan.net/ooxx"
      }
      // 解析html
      case url: String => {
        html ! url
      }
    }
  }

  /**
    * 解析html
    */
  class HtmlActor extends Actor {

    val downactor = context.actorOf(Props[PicActor])

    def receive = {
      case url: String => {
        println("Actor:" + self.toString + " || 解析页面:" + url)
        val html = Jsoup.connect(url).get();
        val body = html.body()
        val navi = body.select("a.previous-comment-page").first()
        val next = navi.attr("href");

        // 通知 JianActor 来创建一个新的HtmlActor解析 next
        sender ! next
        // download
        val comments = body.select("ol.commentlist li")
        for (i <- 0 until comments.size()) {
          val e = comments.get(i)
          if (!e.attr("id").isEmpty()) {
            val p = e.select("p")
            if (p != null) {
              val img = p.first().select("img").first()
              if (img != null) {
                val imgsrc = img.attr("src")
                downactor ! imgsrc
              }
            }
          }
        }
        println("*********解析完毕" + url)
      }
      case _ =>
    }
  }

  class PicActor extends Actor {

    def receive = {
      case url: String => {
        val flag = download(url)
        if (flag) {
          println("Actor:" + self.toString +">>>成功下载："+url)
        }else{
          println("Actor:" + self.toString +">>>失败下载："+url)
        }
        // 下载完毕 通知自己关闭
        self!PoisonPill
      }
    }

    def download(url: String): Boolean = {
      try {
        val name = url.split("/").last
        val u: URL = new URL("http:"+url);
        val is = u.openStream();
        val os = new FileOutputStream("E:\\img\\" + name);
        var bytesRead = 0;
        val buffer = new Array[Byte](8192);
        bytesRead = is.read(buffer, 0, 8192)
        while (bytesRead != -1) {
          os.write(buffer, 0, bytesRead);
          bytesRead = is.read(buffer, 0, 8192)
        }
        return true;
      } catch {
        case e: Exception => {
          e.printStackTrace()
          return false;
        }
      }
    }
  }





}