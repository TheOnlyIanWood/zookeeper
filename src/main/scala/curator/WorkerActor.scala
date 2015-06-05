package curator

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{PoisonPill, Props, Actor}
import akka.actor.Actor.Receive
import akka.event.Logging
import chapter_03.Logger
import curator.Master._
import curator.WorkerActor.{ShutdownGracefully, TaskRequest}
import gui.Util._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

object WorkerActor {

  trait WorkerMessage
  case class TaskRequest(number: Int) extends WorkerMessage
  case object ShutdownGracefully extends WorkerMessage


  def props(name: String, path: String, number: Int): Props = Props(new WorkerActor(name, path, number))
}

class WorkerActor(name: String, znodePath: String, number: Int) extends Actor  {
  val log = Logging(context.system, this)
  log.info(s"Worker [${name}] started")


  private val client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 5))
  client.start()

  override def receive: Receive = {
    case "hello" => log.info(s"helllllllllllo!!!")

      val data = self.path.toString.getBytes
      log.info(s"Worker [${name}] creating path [$znodePath] with data [$data]")
      createPathSync(client, "worker", znodePath, data)

    case "hi" => log.info(s"got a hi")

    case t @ TaskRequest(number)=> log.info(s"Asked to do [$t]")

    case ShutdownGracefully =>
      log.info("Asked to shutdown")
      client.delete.guaranteed.forPath(znodePath)
      context.stop(self)

  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info("postStop xxxxxxxxxxxxxxxxxxxxx ")
    super.postStop()
  }
}
