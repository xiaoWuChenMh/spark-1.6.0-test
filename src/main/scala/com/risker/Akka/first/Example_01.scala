package com.risker.Akka.first

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.event.Logging

/**
  * 通过system.actorOf工厂方法创建的Actor为顶级Actor
  *  在Akka框架中，每个Akka应用程序都会有一个守卫Actor，名称为user，
  *  所有通过system.actorOf工厂方法创建的Actor都为user的子Actor，也是整个Akka程序的顶级Actor。
  *
  * Created by hc-3450 on 2017/4/11.
  */
object Example_01 {

  class MyActor_01 extends Actor{

    //获取LoggingAdapter，用于日志输出
    val log = Logging(context.system, this)

    override def receive  = {
      case "test" => log.info("received test")
      case _  => log.info("received unknown message")
    }

  }

  /**
    * 也可以通过混入ActorLogging来实现日志功能，具体代码如下
    */
  class MyActor_02 extends Actor with ActorLogging {
    override def receive = {
      case "test" => log.info("received test")
      case _ => log.info("received unknown message")
    }
  }

  def main(args: Array[String]): Unit = {
    //创建ActorSystem对象,命名为 myActorSystem
    val system = ActorSystem("myActorSystem")

    //返回ActorSystem的LoggingAdpater
    val systemLog=system.log

    //创建MyActor,指定actor名称为myactor_01，返回的是ActorRef
    val myactor = system.actorOf(Props[MyActor_01],name="myactor_01")

    systemLog.info("准备向myactor发送消息")

    //向myactor发送消息
    myactor!"test"
    myactor! 123

    //关闭ActorSystem，停止程序的运行
    system.shutdown()
  }
}

/**
  * 创建Actor，需要注意的是system.actorOf方法返回的是ActorRef对象，ActorRef为Actor的引用，
  * 使用ActorRef对象可以进行消息的发送等操作.
  *
  * Props为配置对象，在创建Actor时使用，它是不可变的对象，因此它是线程安全且完全可共享的
  *
  * Akka中创建Actor时，也允许直接传入MyActor对象的引用，例如：
  *   val myactor = system.actorOf(Props(new MyActor), name = "myactor")
  * 但是Akka不推荐这么做，官方文档给出的解释是这种方式会导致不可序列化的Props对象且可能会导致竞争条件
  * （破坏Actor的封装性）
  *
  *
  *
  *
  *
  *
  *
  */
