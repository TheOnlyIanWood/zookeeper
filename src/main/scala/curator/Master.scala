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
  val Assign = "/assign"
  val Tasks = "/tasks"


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
  private val tasksCache = new PathChildrenCache(client, Tasks, true)

  //  def doNothing(task: String, data: Array[Byte]) = {}
  //  val doNothing = (task: String, data: Array[Byte]) => {}
  private val doNothing: (String, Array[Byte]) => Unit = (task: String, data: Array[Byte]) => {}

  //  def createATaskZnode(task: String, data: Array[Byte]): Unit = {
  private  val createATaskZnode: (String, Array[Byte]) => Unit = (task: String, data: Array[Byte]) => {
    val path = s"$Tasks/$task"
    log.info(s"createATaskZnode for task [${task}] data [$data] path [$path]")
    client.create.forPath(path, data)
  }

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
      client.create.forPath(Assign, new Array[Byte](0))
      client.create.forPath(Tasks, new Array[Byte](0))
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

  var recoveryLatch = new CountDownLatch(0)

  val tasksCacheListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {

      val path = event.getData.getPath
      log.info(s"path [${path}] type [${event.getType}]")

      event.getType match {
        case CHILD_ADDED =>
          try {
//            assignTask(event.getData.getPath.replaceFirst(s"$Tasks/", ""), event.getData.getData, initialTaskAssignmentCallback, doNothing)
          } catch {
            case e: Exception => log.error("Exception when assigning task.", e) // THIS happens when NOT in background.
          }

        case CHILD_UPDATED =>
        case _ => // TODO perhaps handle this.

      }
    }
  }

  val initialTaskAssignmentCallback = new BackgroundCallback {

    override def processResult(client: CuratorFramework, event: CuratorEvent): Unit = {
      log.info(s"xxx initialTaskAssignmentCallback eventReceived path [${event.getPath}] [${event.getType}]")

      log.info(s"Task assigned correctly [${event.getName}] [$event]")
      //TODO - should not delete if a reassignment can we have a different listener?
      deleteTask(event.getPath.substring(event.getPath.lastIndexOf('-') + 1)) //TODO doesn't land here if called sync, obviously


    }
  }

  val taskReassignmentCallback = new BackgroundCallback {

    override def processResult(client: CuratorFramework, event: CuratorEvent): Unit = {
      log.info(s"xxx taskReassignmentCallback eventReceived path [${event.getPath}] [${event.getType}]")

      log.info(s"Task assigned correctly [${event.getName}] [$event]")
      //TODO - should not delete if a reassignment can we have a different listener?
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
            assignTasks(tasks.asScala.toList, initialTaskAssignmentCallback, doNothing)
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

  def assignTasks(tasks: List[String], callback: BackgroundCallback, noWorkerFun: (String, Array[Byte]) => Unit) = {
    for (task <- tasks) {
      assignTask(task, client.getData.forPath(s"$Tasks/$task"), callback, noWorkerFun)
    }
  }

  def assignTask(task: String, data: Array[Byte], callback: BackgroundCallback, noWorkerFun: (String, Array[Byte]) => Unit) = {
    val workersList = workersCache.getCurrentData
    if (!workersList.isEmpty) {
      val designatedWorker = workersList.get(rand.nextInt(workersList.size)).getPath.replaceFirst(Workers + "/", "")

      val path = s"$Assign/$designatedWorker/$task"
      log.info(s"Assigning task [$task], [${new String(data)}] to worker [$designatedWorker] to path [$path]")
      createAssignment(path, data, callback)
    } else {
      log.warn(s"There are no workers to assign [${task}].")
      //TODO it needs to create a /task entry on a reassign when no workers left.
      noWorkerFun(task,data)
    }
  }

  def createAssignment(path: String, data: Array[Byte], callback: BackgroundCallback) = {
    log.info(s"createAssignment [${path}] [${new String(data)}]")

    client.create.creatingParentsIfNeeded.withMode(CreateMode.PERSISTENT).inBackground(callback).forPath(path, data)
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
        log.info(s"eventReceived path [${event.getPath}] [${event.getType}]")

        event.getType match {
          case CHILDREN => if (event.getPath.contains(Assign)) {

            val tasks = event.getChildren.asScala.toList
            log.info(s"Successfully got a list of assignments of [${tasks.size}] for [${event.getPath}] tasks [${tasks}}].")

            if (!tasks.isEmpty) {
              /*
             We need to get the data first, before deleting so that we can reassign
             */
              val taskDatas = for (task <- tasks) yield (task, client.getData.forPath(s"${event.getPath}/$task"))

              /*
            Delete the assignments of the absent worker
            */
              for (task <- tasks) {
                val path = s"${event.getPath}/$task"
                deletePath(path)
              }

              /*
            * Delete the znode representing the absent worker
            * in the assignments.
            */
              deletePath(event.getPath)

              /*
            * Reassign the tasks.
            */
              for {
                (task, data) <- taskDatas
              } assignTask(task, data, taskReassignmentCallback, createATaskZnode)
            } else {
              log.info(s"No tasks for [${event.getPath}] so nothing to reassign.")
            }

          } else {
            log.warn(s"Unexpected event [${event.getPath}}]")
          }
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

  private[curator] def deleteTask(number: String) {
    val taskPath = s"$Tasks/task-$number"
    log.info(s"Deleting task [$number] path [$taskPath].")
    client.delete.inBackground.forPath(taskPath)

    // this is here to show that all the tasks found on startup have been allocated and then deleted - so
    // it has "recovered"
    recoveryLatch.countDown
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
          try {
            getAbsentWorkerTasks(event.getData.getPath.replaceFirst(Workers + "/", ""))
          } catch {
            case e: Exception => log.error("Exception while trying to re-assign tasks", e)
          }
        case PathChildrenCacheEvent.Type.CHILD_ADDED => {
          val tasks = client.getChildren.forPath(Tasks).asScala.toList
          log.info(s"New worker added [${path}] will assign [${tasks.size}] to him.")
          assignTasks(tasks, initialTaskAssignmentCallback, doNothing)
        }
        case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
        case _ => // TODO perhaps handle this.
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
