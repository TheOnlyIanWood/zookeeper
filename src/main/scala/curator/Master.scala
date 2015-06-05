package curator

import java.io.{Closeable, IOException}
import java.util
import java.util.concurrent.CountDownLatch

import chapter_03.Logger
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.api.CuratorEventType._
import org.apache.curator.framework.api.{BackgroundCallback, CuratorEvent, CuratorListener, UnhandledErrorListener}
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListener}
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.state.ConnectionState._
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.book.recovery.RecoveredAssignments
import org.apache.zookeeper.book.recovery.RecoveredAssignments.RecoveryCallback
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type._
import scala.collection.JavaConverters._
import scala.util.Random

/**
 *
 * create -e /workers/worker-1 "w1"
 * create -e /workers/worker-2 "w2"
 * create -e /workers/worker-3 "w3"
 *
 * create -e /tasks/task-1 "t1"
 * create -e /tasks/task-2 "t2"
 * create -e /tasks/task-3 "t3"
 *
 * delete /workers/worker-1
 * delete /workers/worker-2
 *
 * set /workers/worker-1 "w1-MASSIVE"
 * get /workers/worker-1
 *
 *
 *
 */
object Master extends Logger {

  def main(args: Array[String]) {
    try {
      val master = new Master(args(0), args(1), new ExponentialBackoffRetry(1000, 5))
      master.startZk
      master.bootstrap
      master.runForMaster
      Thread.sleep(Long.MaxValue)
    }
    catch {
      case e: Exception => {
        log.error("Exception while running curator master.", e)
      }
    }
  }

  val Workers = "/workers"

}

/**
 * Created by ian on 13/05/15.
 *
 * //NOTE CuratorMaster and CuratorMasterSelector are more or less the same and use more CountDownLatches.
 * and this is a copy of CuratorMaster
 *
 *
 *
 */
class Master(myId: String, hostPort: String, retryPolicy: RetryPolicy)
  extends Closeable
  with LeaderSelectorListener
  with Logger {

  import Master._

  log.info(s"$myId:$hostPort")

  // NOTE having this below the usages failed as was null, an example of dangerous publishing
  // on non fully constructed object as mentioned in the Goetz and Scala CON book

  private val client = CuratorFrameworkFactory.newClient(hostPort, retryPolicy)
  private val leaderSelector = new LeaderSelector(client, "/master", this)
  private val workersCache = new PathChildrenCache(client, Workers, true)

  /*
  * We use one latch as barrier for the master selection
  * and another one to block the execution of master
  * operations when the ZooKeeper session transitions
  * to suspended.
  */
  private val leaderLatch = new CountDownLatch(1)
  private val closeLatch = new CountDownLatch(1)


  def startZk(): Unit = client.start()

  def bootstrap(): Unit = {
    if (true) {
      // temporary switch to allow rerunning of the Master
      client.create.forPath(Workers, new Array[Byte](0))
      client.create.forPath("/status", new Array[Byte](0))
    }
  }

  def runForMaster {
    leaderSelector.setId(myId)
    log.info("Starting master selection: " + myId)
    leaderSelector.start
  }

  override def close(): Unit = {
    log.info("Closing")
    closeLatch.countDown()
    leaderSelector.close()
    client.close()
  }

  override def takeLeadership(client: CuratorFramework): Unit = {
    log.info("Mastership participants: " + myId + ", " + leaderSelector.getParticipants)

    //Register listeners
    client.getCuratorListenable.addListener(masterListener)
    client.getUnhandledErrorListenable.addListener(errorsListener)

    //start workersCache
    workersCache.getListenable.addListener(workersCacheListener)
    workersCache.start()

    /*
     * This latch is to prevent this call from exiting. If we exit, then
     * we release mastership.
     */
    closeLatch.await()
  }


  val rand = new Random(System.currentTimeMillis)



  /*
   * We use one main listener for the master. The listener processes
   * callback and watch events from various calls we make. Note that
   * many of the events related to workers and tasks are processed
   * directly by the workers cache and the tasks cache.
   */
  val masterListener = new CuratorListener {
    override def eventReceived(client: CuratorFramework, event: CuratorEvent): Unit = {
      try {
        log.info(s"eventReceived path [${event.getPath}] [${event.getType}]")

        event.getType match {
          case CHILDREN =>
            log.warn(s"Unexpected event [${event.getPath}}]")

          case DELETE =>
            log.info(s"Result of delete operation [${event.getResultCode}] [${event.getPath}]")
          /*
           * We delete znodes in two occasions:
           * 1- When reassigning tasks due to a faulty worker;
           * 2- Once we have assigned a task, we remove it from
           *    the list of pending tasks.
           */

          case WATCHED =>
          case _ => log.error(s"Default case [${event.getType}]")
        }
      } catch {
        case e: Exception => log.error(s"Exception while processing event [$event]", e)
          try {
            close()
          } catch {
            case ioe: IOException => log.error("Exception while closing", ioe)
          }
      }
    }
  }

  private def deletePath(path: String): Unit = {
    log.info(s"Deleting [$path].")
    client.delete.inBackground.forPath(path)
  }

  val workersCacheListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {

      val path = event.getData.getPath
      log.info(s"path [${path}] type [${event.getType}]")

      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
                    log.info(s"Worker removed.")

        case PathChildrenCacheEvent.Type.CHILD_ADDED => {
                    log.info(s"New worker added.")
        }
        case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
        case _ => // TODO perhaps handle this.
      }

    }
  }

  val errorsListener = new UnhandledErrorListener {
    override def unhandledError(message: String, e: Throwable): Unit = {
      log.error(s"Unrecoverable error [$message]", e)
      try {
        close()
      } catch {
        case ioe: IOException => log.warn(s"Exception when closing for [$message]", ioe)
      }
    }
  }

  override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
    newState match {
      case CONNECTED => //Nothing to do in this case.
      case RECONNECTED =>
      // Reconnected, so I should
      // still be the leader.
      case SUSPENDED => log.warn("Session suspended");
      case LOST =>
        try {
          close()
        } catch {
          case e: IOException => log.warn("Exception while closing", e);
        }
      case READ_ONLY => // We ignore this case
    }
  }
}
