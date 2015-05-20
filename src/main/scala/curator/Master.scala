package curator

import java.io.{Closeable, IOException}
import java.util
import java.util.concurrent.CountDownLatch

import chapter_03.Logger
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.api.CuratorEventType._
import org.apache.curator.framework.api.{CuratorEvent, CuratorListener, UnhandledErrorListener}
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListener}
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.state.ConnectionState._
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.book.recovery.RecoveredAssignments
import org.apache.zookeeper.book.recovery.RecoveredAssignments.RecoveryCallback

import scala.collection.JavaConverters._
import scala.util.Random


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


}

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

  val Assign = "/assign"

  private val Workers = "/workers"

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
    client.create.forPath(Workers, new Array[Byte](0))
    client.create.forPath(Assign, new Array[Byte](0))
    client.create.forPath("/tasks", new Array[Byte](0))
    client.create.forPath("/status", new Array[Byte](0))
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


  var recoveryLatch = new CountDownLatch(0)

  val tasksCacheListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      if (event.getType == PathChildrenCacheEvent.Type.CHILD_ADDED) {
        try {
          assignTask(event.getData.getPath.replaceFirst("/tasks/", ""), event.getData.getData)
        } catch {
          case e: Exception => log.error("Exception when assigning task.", e)
        }
      }
    }
  }

  override def takeLeadership(client: CuratorFramework): Unit = {
    log.info("Mastership participants: " + myId + ", " + leaderSelector.getParticipants)

    //Register listeners
    client.getCuratorListenable.addListener(masterListener)
    client.getUnhandledErrorListenable.addListener(errorsListener)

    //start workersCache
    workersCache.getListenable.addListener(workersCacheListener)
    workersCache.start()

    new RecoveredAssignments(client.getZookeeperClient.getZooKeeper).recover(new RecoveryCallback {

      override def recoveryComplete(rc: Int, tasks: util.List[String]): Unit = {
        try {
          if (rc == RecoveryCallback.FAILED) {
            log.warn("Recovery of assigned tasks failed.")
          } else {
            log.info("Assigning recovered tasks")
            recoveryLatch = new CountDownLatch(tasks.size)
            assignTasks(tasks.asScala.toList)
          }

          //TODO making a new thread is a bit naff
          new Thread(new Runnable {
            override def run(): Unit = {
              try {
                recoveryLatch.await()

                tasksCache.getListenable.addListener(tasksCacheListener)
                tasksCache.start
              } catch {
                case e: Exception => log.warn("Exception while assigning and getting tasks", e)
              }
            }

          }).start

          leaderLatch.countDown()
        } catch {
          case e: Exception => log.error("Exception while executing the recovery callback", e)
        }
      }
    })

    /*
     * This latch is to prevent this call from exiting. If we exit, then
     * we release mastership.
     */
    closeLatch.await()
  }


  val rand = new Random(System.currentTimeMillis)

  def assignTasks(tasks: List[String]) = {
    for (task <- tasks) {
      assignTask(task, client.getData.forPath(s"/tasks/$task"))
    }
  }


  def assignTask(task: String, data: Array[Byte]) = {
    val workersList = workersCache.getCurrentData
    val designatedWorker = workersList.get(rand.nextInt(workersList.size)).getPath.replaceFirst(Workers+"/", "")
    log.info(s"Assigning task [$task], [${new String(data)}}] to worker [$designatedWorker]")

    val path = s"/assign/$designatedWorker/$task"
    createAssignment(path, data)

  }

  def createAssignment(path: String, data: Array[Byte]) = {
    log.info(s"createAssignment [${path}] [${new String(data)}}]")
    /*
      * The default ACL is ZooDefs.Ids#OPEN_ACL_UNSAFE
      */
    client.create.withMode(CreateMode.PERSISTENT).inBackground.forPath(path, data)
  }

  /*
   * We use one main listener for the master. The listener processes
   * callback and watch events from various calls we make. Note that
   * many of the events related to workers and tasks are processed
   * directly by the workers cache and the tasks cache.
   */
  val masterListener = new CuratorListener {
    override def eventReceived(client: CuratorFramework, event: CuratorEvent): Unit = {
      try {
        log.info(s"eventReceived path [${event.getPath}]")

        event.getType match {
          case CHILDREN => if (event.getPath.contains(Assign)) {
            log.info(s"Successfully got a list of assignments of [${event.getChildren.size}] tasks.")

            /*
            Delete the assignments of the absent worker
             */
            for (task <- event.getChildren.asScala.toList) {
              deleteAssignment(s"${event.getPath}/$task")
            }

            /*
            * Delete the znode representing the absent worker
            * in the assignments.
            */
            deleteAssignment(event.getPath)

            /*
            * Reassign the tasks.
            */
            assignTasks(event.getChildren.asScala.toList)

          } else {
            log.warn(s"Unexpected event [${event.getPath}}]")
          }
          case CREATE =>
            if (event.getPath.contains(Assign)) {
              log.info(s"Task assigned correctly [${event.getName}] [$event]")
              deleteTask(event.getPath.substring(event.getPath.lastIndexOf('-') + 1))
            }
          case DELETE =>
            /*
             * We delete znodes in two occasions:
             * 1- When reassigning tasks due to a faulty worker;
             * 2- Once we have assigned a task, we remove it from
             *    the list of pending tasks.
             */
            if (event.getPath.contains("/tasks")) {
              log.info(s"Result of delete operation [${event.getResultCode}] [${event.getPath}]")
            } else if (event.getPath.contains(Assign)) {
              log.info(s"Task correctly deleted [${event.getPath}]")
            }
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

  private[curator] def deleteTask(number: String) {
    val taskPath = s"/tasks/task-$number"
    log.info(s"Deleting task [$number] path [$taskPath].")
    client.delete.inBackground.forPath(taskPath)
    recoveryLatch.countDown
  }

  private def deleteAssignment(path: String): Unit = {
    log.info(s"Deleting assignment [$path].")
    client.delete.inBackground.forPath(path)
  }

  val workersCacheListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      if (event.getType == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
        /*
         * Obtain just the worker's name
         */
        try {
          getAbsentWorkerTasks(event.getData.getPath.replaceFirst(Workers+"/", ""))
        } catch {
          case e: Exception => log.error("Exception while trying to re-assign tasks", e)
        }
      }

    }
  }

  private def getAbsentWorkerTasks(worker: String): Unit = {
    client.getChildren.inBackground.forPath(s"$Assign/$worker")
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
