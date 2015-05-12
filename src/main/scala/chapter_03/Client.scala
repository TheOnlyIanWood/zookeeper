package chapter_03

import org.apache.zookeeper.AsyncCallback.StringCallback
import org.apache.zookeeper.KeeperException.{Code, NodeExistsException}
import org.apache.zookeeper.{KeeperException, CreateMode, ZooDefs, WatchedEvent}

/**
 * Created by ian on 12/05/15.
 */
object Client extends Logger {

  def main(args: Array[String]): Unit = {
    val c = new Client(args(0))
    c.startZk()

    val name = c.queueCommand(args(1))
    log.info(s"Created [$name]")

    Thread.sleep(30 * 1000L)
  }
}

class Client(hostPort: String) extends DefaultWatcher(hostPort) with Logger {

  override def process(event: WatchedEvent): Unit = log.info(event.toString)


  def queueCommand(command: String): Unit = {
    zk.create("/tasks/task-",
      command.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
      CreateMode.EPHEMERAL_SEQUENTIAL, queueCommandCallBack, null)
  }

  def queueCommandCallBack = new StringCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, name: String): Unit = {
      Code.get(rc) match {
        case Code.OK => log.info(s"Registered successfully: $name")
        case Code.NODEEXISTS => log.warn(s"Already registered: $name")
        case _ => log.error(s"Something went wrong: ${KeeperException.create(Code.get(rc), path)}")
      }
    }
  }

}
