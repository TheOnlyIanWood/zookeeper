package curator

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{PoisonPill, Props, Actor}
import akka.actor.Actor.Receive
import akka.event.Logging
import chapter_03.Logger
import curator.Master._
import curator.WorkerActor.{ShutdownGracefully, TaskRequest}
import curator.WorkerType.WorkerType
import curator.gui.MasterActor.CreatedWorker
import gui.Util._
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.curator.retry.ExponentialBackoffRetry


object WorkerType {
  sealed trait WorkerType

  case object Big extends WorkerType
  case object Small extends WorkerType

  val workerTypes = Seq(Big, Small)
}

object WorkerActor {

  sealed trait WorkerMessage
  case class TaskRequest(number: Int) extends WorkerMessage
  case object ShutdownGracefully extends WorkerMessage

  def props(path: String, number: Int, workerType: WorkerType): Props = Props(new WorkerActor(path, number,workerType))
}




class WorkerActor(znodePath: String, number: Int, workerType: WorkerType) extends Actor with ConnectionStateListener  {
  import WorkerActor._
  val log = Logging(context.system, this)
  log.info(s"Worker [${self.path.name}] started. I am [$workerType].")


  private val client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 5))
  client.getConnectionStateListenable.addListener(this)
  client.start()

  override def receive: Receive = {
    case "hello" => log.info(s"helllllllllllo!!!")

      val data = self.path.toString.getBytes
      log.info(s"Worker [${self.path.name}] creating path [$znodePath] with data [$data]")
      createPathSync(client, "worker", znodePath, data)


      sender ! CreatedWorker

    case "hi" => log.info(s"got a hi")

    case t @ TaskRequest(number)=> log.info(s"Asked to do [$t]")
//      sender ! "result" //TODO tmp stop response as dead letters as sender not yet the master actor

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

  override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
    log.info(s"newState [${newState}]")
  }
}
