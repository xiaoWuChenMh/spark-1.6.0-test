package com.risker.Akka.second

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}

/**
  * 成员变量self及成员方法sender方法的使用
  * Created by hc-3450 on 2017/4/13.
  */
object Example_04 {

  class FirstActor extends  Actor with ActorLogging{
    //通过context.actorOf方法创建Actor
    var child:ActorRef = _
    override def preStart():Unit = {
      log.info("preStart() in FirstActor")
      //通过context上下文创建Actor
      child = context.actorOf(Props[MyActor],name="MyActor")
    }
    override def receive = {
      //向MyActor发送消息
      case x => child ! x;log.info("received "+x)
    }
  }

  class MyActor extends Actor with ActorLogging{
    self!"message from self reference" //向自身发送了一条消息
    def receive = {
      case "test" => {
        log.info("received test")
        //向sender（[发信人]本例中为FirstActor）发送”message from MyActor”消息
        sender!"message from MyActor"
      }
      case "message from self reference"=>log.info("message from self refrence")
      case _      => log.info("received unknown message");
    }

  }

  def main(args: Array[String]): Unit = {

    val system = ActorSystem("MyActorSystem04")
    val systemLog=system.log
    //创建FirstActor对象
    val myactor =system.actorOf(Props[FirstActor],name="FirstActor")
    systemLog.info("准备向myactor发送消息")
    //向myactor发送消息
    myactor!"test"
    myactor! 123
    Thread.sleep(5000)
    //关闭ActorSystem，停止程序的运行
    system.shutdown()

  }

}
