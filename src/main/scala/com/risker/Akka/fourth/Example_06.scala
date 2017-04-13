package com.risker.Akka.fourth

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}

/**
  * 通过ActorSystem.shutdown方法停止所有 Actor的运行
  * Created by hc-3450 on 2017/4/13.
  */
object Example_06 {

  class  FirstActor extends Actor with ActorLogging{

    var child:ActorRef = context.actorOf(Props[MyActor], name = "myActor")
    def receive = {
      //向MyActor发送消息
      case x => child ! x;log.info("received "+x)
    }
    override def postStop(): Unit = {
      log.info("postStop In FirstActor")
    }
  }

  class MyActor extends Actor with ActorLogging{
    def receive = {
      case "test" => log.info("received test");
      case _      => log.info("received unknown message");
    }
    override def postStop(): Unit = {
      log.info("postStop In MyActor")
    }
  }


  def main(args: Array[String]): Unit = {
    val system = ActorSystem("MyActorSystem")
    val systemLog=system.log
    //创建FirstActor对象
    val firstactor = system.actorOf(Props[FirstActor], name = "firstActor")
    systemLog.info("准备向firstactor发送消息")
    //向firstactor发送消息
    firstactor!"test"
    firstactor! 123
    //关闭ActorSystem，停止所有Acotr运行
    system.shutdown()
  }

}
