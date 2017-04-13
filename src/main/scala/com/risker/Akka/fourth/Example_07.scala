package com.risker.Akka.fourth

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}

/**
  * 通过context.stop方法停止Actor的运行
  * Created by hc-3450 on 2017/4/13.
  */
object Example_07 {

  class FirstActor extends Actor with ActorLogging{

    var child:ActorRef = context.actorOf(Props[MyActor], name = "myActor")
    def receive = {
      case "stop"=> Thread.sleep(5000);context.stop(child)//关闭child对应的Actor
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
