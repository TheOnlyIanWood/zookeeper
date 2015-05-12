package chapter_03

import java.util.Date

import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{WatchedEvent, Watcher}
import scala.collection.JavaConverters._

object AdminClient{

  def main(args: Array[String]): Unit ={
    val c = new AdminClient(args(0))
    c.startZk()
    c.listState()
  }
}

class AdminClient(hostPort: String) extends DefaultWatcher(hostPort) {
  override def process(event: WatchedEvent): Unit = {
    log.info(event.toString)
  }

  def listState(): Unit = {
    val stat = new Stat()
    val masterData = zk.getData(Master.Master, false, stat)
    val startDate = new Date(stat.getCtime)
    log.info(s"Master: ${masterData.toString} since $startDate")

    log.info("Workers:")
    zk.getChildren("/workers", false).asScala.foreach { w =>
      val state = zk.getData(s"/workers/$w", false, null).toString
      log.info(s"\t$w:$state")
    }

    log.info("Tasks:")
    zk.getChildren("/tasks", false).asScala.foreach { t =>
      log.info(s"\t$t")
    }

  }

}
