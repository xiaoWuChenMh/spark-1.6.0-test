package com.risker.Akka.first

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.event.Logging

/**
  * 通过调用context.actorOf创建Actor的展示
  * Created by hc-3450 on 2017/4/11.
  */
object Example_02 {

    class MyChild extends Actor{

      val log = Logging(context.system, this)

      override def receive  = {
        case "test" => log.info("received test")
        case _      => log.info("received unknown message")
      }

    }


    class FirstActor  extends Actor with ActorLogging{

      //通过调用context.actorOf创建Actor
      val child = context.actorOf(Props[MyChild], name = "myChild")

      override def receive  = {
        case x => child ! x;log.info("received "+x)
      }
    }

    def main(args: Array[String]): Unit = {
      //创建ActorSystem对象,命名为 myActorSystem
      val system = ActorSystem("myActorSystem")

      //返回ActorSystem的LoggingAdpater
      val systemLog=system.log

      //创建MyActor,指定actor名称为myactor_03 ，返回的是ActorRef
      val myactor = system.actorOf(Props[FirstActor],name="FirstActor")

      systemLog.info("准备向myactor发送消息")

      //向myactor发送消息
      myactor!"test"
      myactor! 123

      //关闭ActorSystem，停止程序的运行
      system.shutdown()
    }

}

/**
  * 代码使用context.actorOf创建的MyActor，其Actor路径信息为
  * [akka://myActorSystem/user/myactor_03/myChild]，这意味着myChild为myactor_03的子Actor
  *
  * 总结：也就是说context.actorOf和system.actorOf的差别是system.actorOf创建的actor为顶级Actor，
  * 而context.actorOf方法创建的actor为调用该方法的Actor的子Actor
  *
  *
  */

