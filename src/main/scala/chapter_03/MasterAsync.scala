package chapter_03

import org.apache.zookeeper.AsyncCallback.{DataCallback, StringCallback}
import org.apache.zookeeper.KeeperException.{NoNodeException, Code}
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat
import scala.util.Random
import org.slf4j.LoggerFactory

trait Logger {
  val log = LoggerFactory.getLogger(this.getClass)

}

object MasterAsync {


  def main(args: Array[String]): Unit = {
    val m = new MasterAsync(args(0))
    m.startZk()
    m.runForMaster()
    Thread.sleep(60000)

    m.stopZk()
  }
}

class MasterAsync(hostPort: String) extends Watcher with Logger {

  var isLeader = false

  val Master = "/master"

  val serverId = Integer.toHexString(new Random().nextInt()).getBytes()

  private lazy val zk: ZooKeeper = new ZooKeeper(hostPort, 15000, this)

  def startZk(): Unit = {
    log.info(s"startZk [$zk]")
  }

  def masterCreateCallBack = new StringCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, name: String): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS => checkMaster()
        case Code.OK =>
          isLeader = true
          bootstrap()
        case _ => isLeader = false
      }

      log.info(s"I am ${if (isLeader) "" else " not "}the leader")
    }
  }

  def checkMaster(): Unit = {
    println("checkForMaster")
    zk.getData(Master, false, masterCheckCallback, null)

  }

  def runForMaster() = {
    zk.create(Master, serverId, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallBack, null)
  }

  def masterCheckCallback = new DataCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, data: Array[Byte], stat: Stat): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS => checkMaster()
        case Code.NONODE => runForMaster()
      }
    }

  }

  override def process(event: WatchedEvent): Unit = println(s"xxxxx [$event].")

  def bootstrap(): Unit = {
    log.info("bootstrap")
    createParent("/workers")
    createParent("/assign")
    createParent("/tasks")
    createParent("/status")
  }

  def createParent(path: String, data: Array[Byte] = Array[Byte]()): Unit = {
    log.info(s"Creating parent [$path]")
    zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data)
  }

  def createParentCallback = new StringCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, name: String): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS => createParent(path, ctx.asInstanceOf[Array[Byte]])
        case Code.OK => log.info(s"Parent created [$path].")
        case Code.NODEEXISTS => log.warn(s"Parent already exists [$path].")
      }
    }
  }

  def stopZk(): Unit = {
    zk.close()
  }

}
