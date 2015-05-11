package chapter_03


import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException}
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat

import scala.util.Random

/**
 * use:
 *
-Dlog4j.configuration=file:///opt/zookeeper/zookeeper-3.4.6/conf/log4j.properties
127.0.0.1:2181
 *
 */
object Master {

  def main(args: Array[String]): Unit = {
    val m = new Master(args(0))
    m.startZk()

    if (m.runForMaster()) {
      println("I am the leader")
      Thread.sleep(60000)
    } else {
      println("someone else is the leader")
    }



    m.stopZk()
  }

}

/**
 * Created by ian on 11/05/15.
 */
class Master(hostPort: String) extends Watcher {

  //  var isLeader = false
  val r = new Random()
  val serverId = Integer.toHexString(r.nextInt()).getBytes()

  var zk: ZooKeeper = null

  def startZk(): Unit = {
    zk = new ZooKeeper(hostPort, 15000, this)
  }

  def checkForMaster(): Boolean = {
    while (true) {
      try {
        val stat = new Stat
        val data = zk.getData("/master", false, stat)
        new String(data).equals(serverId)
      } catch {
        case e: NoNodeException => false
      }
    }
    false
  }

  def runForMaster(): Boolean = {
    var continue = true
    while (continue) {
      try {
        zk.create("/master", serverId, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        continue = false
        return true
      } catch {
        case e: NodeExistsException => {
          println(s"xxx NodeExistsException $e")
          return false
        }
        case e: InterruptedException => {

          println(s"xxx InterruptedException $e")
        }
      }
    }

    checkForMaster()

  }

  override def process(event: WatchedEvent): Unit = println(s"xxxxx [$event].")

  def stopZk(): Unit = {
    zk.close()
  }


}
