package com.risker.Akka.second

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}

/**
  * Created by hc-3450 on 2017/4/13.
  */
object Example_05 {

  class FirstActor extends Actor with ActorLogging{

    override def receive: Receive = {
      case "test" => log.info("received test")
    }

    override def unhandled(message: Any): Unit = {
      log.info("unhandled message is {}",message)
    }
  }

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("myActorSystem05")
    val systemLog = system.log
    val myactor = system.actorOf(Props[FirstActor],name="FirstActor")
    systemLog.info("准备向myactor发送消息")
    //向myactor发送消息
    myactor!"test"
    myactor! 123
    Thread.sleep(5000)
    //关闭ActorSystem，停止程序的运行
    system.shutdown()
  }

}
