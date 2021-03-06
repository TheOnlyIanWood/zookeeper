
package chapter_03


import org.apache.zookeeper.KeeperException.{ConnectionLossException, NoNodeException, NodeExistsException}
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
object MasterSync {

  def main(args: Array[String]): Unit = {
    val m = new MasterSync(args(0))
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

/**
 * Created by ian on 11/05/15.
 */
class MasterSync(hostPort: String) extends DefaultWatcher(hostPort) {

  val Master = "/master"

  def checkForMaster(): Boolean = {
    println("checkForMaster")
    while (true) {
      try {
        val stat = new Stat
        val data = zk.getData(Master, false, stat)
        return new String(data).equals(serverId)
      } catch {
        case e: NoNodeException => return false
      }
    }
    false
  }

  //TODO try to simulate task/worker pattern
  //1) Worker
  //2) ServiceWantingWorkDone
  //3) WorkerController
  //4) Tasks
  //5) TaskResults


  def runForMaster(): Boolean = {

    while (true) {
      try {
        zk.create(Master, serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        return true
      } catch {
        case e: NodeExistsException =>
          println(s"xxx NodeExistsException $e")
          return false
        case e: ConnectionLossException => {
          if (checkForMaster()) return true
        }
      }
    }
    false

  }

  override def process(event: WatchedEvent): Unit = println(s"xxxxx [$event].")

  def stopZk(): Unit = {
    zk.close()
  }


}
