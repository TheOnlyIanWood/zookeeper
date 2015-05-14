package curator

import java.io.{Closeable, IOException}
import java.util

import chapter_03.Logger
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.api.{CuratorEvent, CuratorListener, UnhandledErrorListener}
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListener}
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.state.ConnectionState._
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.zookeeper.book.recovery.RecoveredAssignments
import org.apache.zookeeper.book.recovery.RecoveredAssignments.RecoveryCallback

/**
 * Created by ian on 13/05/15.
 */
class Master(myId: String, hostPort: String, retryPolicy: RetryPolicy)
  extends Closeable
  with LeaderSelectorListener
  with Logger {

  log.info(s"$myId:$hostPort")

  private val client = CuratorFrameworkFactory.newClient(hostPort, retryPolicy)
  private val leaderSelector = new LeaderSelector(client, "/master", this)
  private val workersCache = new PathChildrenCache(client, "/workers", true)
  private val tasksCache = new PathChildrenCache(client, "/tasks", true)

  def startZk(): Unit = client.start()

  def bootstrap(): Unit = {
    client.create.forPath("/workers", new Array[Byte](0))
    client.create.forPath("/assign", new Array[Byte](0))
    client.create.forPath("/tasks", new Array[Byte](0))
    client.create.forPath("/status", new Array[Byte](0))
  }

  def runForMaster {
    leaderSelector.setId(myId)
    log.info("Starting master selection: " + myId)
    leaderSelector.start
  }

  override def close(): Unit = ???

  override def takeLeadership(client: CuratorFramework): Unit = {
    log.info("Mastership participants: " + myId + ", " + leaderSelector.getParticipants)

    //Register listeners
    client.getCuratorListenable.addListener(masterListener)
    client.getUnhandledErrorListenable.addListener(errorsListener)

    //start workersCache
    workersCache.getListenable.addListener(workersCacheListener)
    workersCache.start()

    new RecoveredAssignments(client.getZookeeperClient.getZooKeeper).recover(new RecoveryCallback {
      override def recoveryComplete(rc: Int, tasks: util.List[String]): Unit = ???
    })

  }

  val masterListener = new CuratorListener {
    override def eventReceived(client: CuratorFramework, event: CuratorEvent): Unit = ???
  }

  val errorsListener = new UnhandledErrorListener {
    override def unhandledError(message: String, e: Throwable): Unit = ???
  }

  val workersCacheListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = ???
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
