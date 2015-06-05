package curator

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Props, Actor}
import akka.actor.Actor.Receive
import chapter_03.Logger
import curator.Master._
import gui.Util._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

object WorkerActor {
  def props(name: String, path: String, number: Int): Props = Props(new WorkerActor(name, path, number))
}

class WorkerActor(name: String, znodePath: String, number: Int) extends Actor with Logger {
  log.info(s"Worker [${name}] started")


  private val client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 5))
  client.start()

  override def receive: Receive = {
    case "hello" => log.info(s"helllllllllllo!!!")

      val data = self.path.toString.getBytes
      log.info(s"Worker [${name}] creating path [$znodePath] with data [$data]")
      createPathSync(client, "worker", znodePath, data)

    case "hi" => log.info(s"got a hi")

  }
}
