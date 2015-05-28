package curator
package gui

import java.util.concurrent.atomic.AtomicInteger

import chapter_03.Logger
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import rx.lang.scala._

import scala.swing._
import scala.swing.event._
import scala.util.control.NonFatal

/**
 * Created by ian on 28/05/15.
 */
object ClientGui extends scala.swing.SimpleSwingApplication with Logger {

  import Master._

  private val client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 5))

  client.start()

  private val tasksCache = new PathChildrenCache(client, Tasks, true)

  private val taskNumber = new AtomicInteger(1)

  val tasksCacheListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {

      val path = event.getData.getPath
      log.info(s"path [${path}] type [${event.getType}]")


    }
  }

  tasksCache.getListenable.addListener(tasksCacheListener)
  tasksCache.start


  def top = new MainFrame {
    title = "Client Gui"

    val button = new Button {
      text = "Click"
    }

    contents = button

    val buttonClicks = Observable[Unit] { sub =>
      button.reactions += {
        case ButtonClicked(_) => sub.onNext(())
      }
    }

    buttonClicks.subscribe(_ => log.info("button clicked"))

    buttonClicks.subscribe { _ =>
      val nextNumber = taskNumber.getAndIncrement()
      val taskPath = s"$Tasks/task-$nextNumber"
      log.info(s"going to make $taskPath")
      try {
        client.create.forPath(taskPath, new Array[Byte](nextNumber))
      } catch {
        case NonFatal(e) => log.info(s"Problem creating [$taskPath]", e)
      }
    }
  }
}
