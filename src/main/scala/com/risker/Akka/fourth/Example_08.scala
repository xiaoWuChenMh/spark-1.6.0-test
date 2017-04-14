package com.risker.Akka.fourth

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props}

/**
  * 通过akka.actor.PoisonPill消息停止指定Actor的运行
  * Created by hc-3450 on 2017/4/14.
  */
object Example_08 {

  class FirstActor extends Actor with ActorLogging{

    var child:ActorRef = context.actorOf(Props[MyActor], name = "myActor")
    def receive = {
      //向child发送PoisonPill停止其运行
      case "stop"=>Thread.sleep(500);child!PoisonPill
      case x =>{
        //向MyActor发送消息
        child ! x
        log.info("received "+x)
      }

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
    firstactor!"stop"
  }
}
