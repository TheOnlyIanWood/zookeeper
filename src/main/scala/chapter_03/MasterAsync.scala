package chapter_03

import org.apache.zookeeper.AsyncCallback.{DataCallback, StringCallback}
import org.apache.zookeeper.KeeperException.{NoNodeException, Code}
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat

import scala.util.Random

object MasterAsync {


  def main(args: Array[String]): Unit = {
    val m = new Master(args(0))
    m.startZk()

    if (m.runForMaster()) {
      println("I am the leader")
      Thread.sleep(60000)
    } else {
      println("Someone else is the leader")
    }

    m.stopZk()
  }
}

class MasterAsync(hostPort: String) extends Watcher {

  var isLeader = false

  val Master = "/master"

  val serverId = Integer.toHexString(new Random().nextInt()).getBytes()

  private lazy val zk: ZooKeeper = new ZooKeeper(hostPort, 15000, this)

  def startZk(): Unit = {
    println(s"startZk [$zk]")
  }

  def masterCreateCallBack = new StringCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, name: String): Unit = {
      Code.get(rc) match {
        case Code.CONNECTIONLOSS => checkMaster()
        case Code.OK => isLeader = true
        case _ => isLeader = false
      }

      println(s"I am ${if (isLeader) "" else " not "} the leader")
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
}
