package chapter_03


import org.apache.zookeeper.{Watcher, ZooKeeper, WatchedEvent}

/**
 * use:
 *
-Dlog4j.configuration=file:///opt/zookeeper/zookeeper-3.4.6/conf/log4j.properties
127.0.0.1:2181
 *
 */
object Master {

  def main(args: Array[String]): Unit ={
   val m = new Master(args(0))
    m.startZk()

    Thread.sleep(60000)
  }

}

/**
 * Created by ian on 11/05/15.
 */
class Master(hostPort: String) extends Watcher {

  var zk: ZooKeeper = null

  def startZk(): Unit = {
    zk = new ZooKeeper(hostPort, 15000, this)
  }

  override def process(event: WatchedEvent): Unit = println(s"xxxxx [$event].")



}
