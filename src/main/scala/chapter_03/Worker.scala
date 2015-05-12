package chapter_03

import com.sun.xml.internal.fastinfoset.algorithm.BuiltInEncodingAlgorithm.WordListener
import org.apache.zookeeper.AsyncCallback.{StatCallback, StringCallback}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat

import scala.util.Random

/**
 * Created by ian on 12/05/15.
 */
abstract class DefaultWatcher(hostPort: String) extends Watcher with Logger {

  val serverId = Integer.toHexString(new Random().nextInt())

  lazy val zk: ZooKeeper = new ZooKeeper(hostPort, 15000, this)

  def startZk(): Unit = {
    println(s"startZk [$zk]")
  }
}

object Worker {
  def main(args: Array[String]): Unit = {
    val w = new Worker(args(0))
    w.startZk()
    w.register()
    Thread.sleep(30 * 1000L)
  }

}

class Worker(hostPort: String) extends DefaultWatcher(hostPort) {

  override def process(event: WatchedEvent): Unit = {
    log.info(s"${event.toString}, $hostPort")
  }

  def register(): Unit = {
    name = s"worker-$serverId"
    zk.create(s"/workers/$name",
      "idle".getBytes(),
      ZooDefs.Ids.OPEN_ACL_UNSAFE,
      CreateMode.EPHEMERAL,
      createWorkerCallback,
      null)
  }

  def createWorkerCallback = new StringCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, name: String): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS => register()
        case Code.OK => log.info(s"Registered successfully: $serverId")
        case Code.NODEEXISTS => log.warn(s"Already registered: $serverId")
        case _ => log.error(s"Something went wrong: ${KeeperException.create(Code.get(rc), path)}")
      }
    }
  }

  def statusUpdateCallBack = new StatCallback {

    override def processResult(rc: Int, path: String, ctx: scala.Any, stat: Stat): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS => updateStatus(ctx.toString)
      }
    }

  }

  var status: String = ""
  var name = ""

 private def updateStatus(status: String): Unit = {
   synchronized {
     if (status == this.status) {
       zk.setData(s"/workers/$name", status.getBytes(), -1, statusUpdateCallBack, status)
     }
   }
  }

  def setStatus(status: String) : Unit ={
    this.status=status
    updateStatus(status)
  }

}
