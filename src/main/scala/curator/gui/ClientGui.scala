package curator
package gui

import java.util
import java.util.concurrent.atomic.AtomicInteger

import chapter_03.Logger
import org.apache.curator.framework.api.transaction.{CuratorTransactionResult, CuratorTransaction, CuratorTransactionFinal}
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import rx.lang.scala._

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.swing._
import scala.swing.event._
import scala.util.{Try, Failure, Success}
import scala.util.control.NonFatal
import scala.concurrent.ExecutionContext.Implicits.global


/**
 * Created by ian on 28/05/15.
 */
object ClientGui extends scala.swing.SimpleSwingApplication with Logger {

  import Master._

  private val client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 5))

  client.start()

  private val tasksCache = new PathChildrenCache(client, Tasks, true)

  private val taskNumber = new AtomicInteger(1)
  private val workerNumber = new AtomicInteger(1)

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
    private val AddWorkerText = "Add Worker"

    private val addTaskButton = new Button {      text = "Add Task"    }
    private val addWorkerButton = new Button {       text = AddWorkerText    }
    private val deleteTasksButton = new Button {      text = "Delete Tasks"    }
    private val deleteWorkersButton = new Button {      text = "Delete Workers"    }
    private val deleteAssignmentsButton = new Button {      text = "Delete Assignments"    }

    contents = new BorderPanel {

      import BorderPanel.Position._

      layout(new BorderPanel {
        layout(addTaskButton) = West
        layout(addWorkerButton) = East
        layout(deleteTasksButton) = South
        layout(deleteWorkersButton) = North
        layout(deleteAssignmentsButton) = Center
      }) = North
    }

    val taskButtonClicks = Observable[Unit] { sub =>
      addTaskButton.reactions += {
        case ButtonClicked(_) => sub.onNext(())
      }
    }

    val workerButtonClicks = Observable[Unit] { sub =>
      addWorkerButton.reactions += {
        case ButtonClicked(_) => sub.onNext(())
      }
    }

    val deleteTasksButtonClicks = Observable[Unit] { sub =>
      deleteTasksButton.reactions += {
        case ButtonClicked(_) => sub.onNext(())
      }
    }

    val deleteWorkersButtonClicks = Observable[Unit] { sub =>
      deleteWorkersButton.reactions += {
        case ButtonClicked(_) => sub.onNext(())
      }
    }

    val deleteAssignmentsButtonClicks = Observable[Unit] { sub =>
      deleteAssignmentsButton.reactions += {
        case ButtonClicked(_) => sub.onNext(())
      }
    }

    taskButtonClicks.subscribe(_ => log.info("button clicked"))

    taskButtonClicks.subscribe { _ =>
      val nextNumber = taskNumber.getAndIncrement()
      val path = s"$Tasks/task-$nextNumber"
      createPath("task", path, nextNumber)
    }

    workerButtonClicks.subscribe(_ => log.info("button clicked"))

    workerButtonClicks.subscribe { _ =>
      val nextNumber = workerNumber.getAndIncrement()
      val path = s"$Workers/worker-$nextNumber"
      createPath("worker", path, nextNumber)
      addWorkerButton.text = s"$AddWorkerText-$nextNumber"
    }

    private def createPath(comment: String, path: String, number: Int) = {
      log.info(s"going to make [$comment] [$path]")
      try {
        client.create.forPath(path, number.toString.getBytes)
      } catch {
        case NonFatal(e) => log.info(s"Problem creating [$path]", e)
      }
    }

    deleteTasksButtonClicks.subscribe { _ =>   deleteChildren(Tasks)  }

    deleteWorkersButtonClicks.subscribe { _ => deleteChildren(Workers)    }

    def deleteChildren(path: String) = {
      log.info(s"deleteChildren path [${path}]")
      val children = client.getChildren.forPath(path).asScala

      for (child <- children) {
        val childPath = s"$path/$child"
        log.info(s"Deleting [$childPath]")
        client.delete.forPath(childPath)
      }

    }


    deleteAssignmentsButtonClicks.subscribe { _ =>
      log.info(s"DeleteAssignments BEFORE XXXX")
      val future = deleteAssignments()
      future.onComplete {
        case Success(results) =>
          for (result <- results) {
            log.info(s"result [${result.getForPath}] [${result.getType}]")
          }
          log.info(s"Deleted [${results.size}] assignments.")
        case Failure(f) => log.warn("Delete failed", f)
      }
      log.info(s"deleteAssignments END XXXX")
    }

    def deleteAssignments(): Future[List[CuratorTransactionResult]] = {
      Future {
          val transaction = client.inTransaction()
          val workers = client.getChildren.forPath(Assign).asScala
          log.info(s"workers [${workers}]")

          for {
            worker <- workers
            path = s"$Assign/$worker"
          } {
            deleteChildren(path)
            log.info(s"deleting worker [$path]")
            if (path == "/assign/worker-5") {
              throw new Exception("Hopefully fail transaction")
            }
            delete(path, transaction)
          }

          def deleteChildren(worker: String) = {
            log.info(s"deleteChildren for  worker [${worker}]")
            for {
              task <- client.getChildren.forPath(worker).asScala
            } {
              val taskPath = s"$worker/$task"
              log.info(s"deleting task [$taskPath]")
              delete(taskPath, transaction)
            }
          }

        /**
         * These results are in the same order as added.
         */
        val results: util.Collection[CuratorTransactionResult] = transaction.asInstanceOf[CuratorTransactionFinal].commit()
        results.asScala.toList // doing toList Scala impl for an iterator is a stream.
      }
    }

    //TODO this is copied from Master, try to add to trait and mix in.
    def delete(path: String, transaction: CuratorTransaction): CuratorTransactionFinal = {
      transaction.delete.forPath(path).and()
    }

  }
}

