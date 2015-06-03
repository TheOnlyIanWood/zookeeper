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
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type._
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.swing._
import scala.swing.event._
import scala.util.{Try, Failure, Success}
import scala.util.control.NonFatal
import scala.concurrent.ExecutionContext.Implicits.global


/**
 * Created by ian on 28/05/15.
 */
object ClientGui extends scala.swing.SimpleSwingApplication {

  abstract class GuiFrame extends MainFrame {
    title = "Client Gui"
    val AddWorkerText = "Add Worker"
    val AddTaskText = "Add Task"

    val addTaskButton = new Button {      text = AddTaskText    }
    val addWorkerButton = new Button {     text = AddWorkerText    }
    val deleteTasksButton = new Button {      text = "Delete Tasks"    }
    val deleteWorkersButton = new Button {      text = "Delete Workers"    }
    val deleteAssignmentsButton = new Button {      text = "Delete Assignments"    }
    val resultsFields = new TextArea
    val scrollPane = new ScrollPane(resultsFields)

    contents = new BorderPanel {

      import BorderPanel.Position._

      layout(new BorderPanel {
        layout(addTaskButton) = West
        layout(addWorkerButton) = East
        layout(deleteTasksButton) = South
        layout(deleteWorkersButton) = North
        layout(deleteAssignmentsButton) = Center

      }) = North
      layout(scrollPane) = Center
    }
  }

  trait GuiLogic {
    self: GuiFrame =>

    import Master._


    private val client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 5))

    client.start()


    private val taskNumber = new AtomicInteger(0) //using incrementAndGet so first will be 1
    private val workerNumber = new AtomicInteger(0)

    def handleChildEvent(client: CuratorFramework, event: PathChildrenCacheEvent, entityType: String, button: Button, ai: AtomicInteger) = {

      val path = event.getData.getPath

      val number = path.split("-")(1).toInt

      val eventType = event.getType
      val eventDescription = s"path [${path}] type [${eventType}] Number [$number]"
      log.info(eventDescription)
      eventType match {
        case CHILD_ADDED => if (number > ai.get()) {
          log.info(s"Setting $entityType number to [${number}]")
          ai.set(number)
          button.text = s"$AddTaskText-$number" // TODO this is not on AWT thread.
        }
        case _ => log.info(s"not handling [${eventDescription}]")

      }
    }

    private val tasksCache = new PathChildrenCache(client, Tasks, true)
    tasksCache.getListenable.addListener(new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
        handleChildEvent(client, event, "task", addTaskButton, taskNumber)
      }
    })
    tasksCache.start()

    private val workersCache = new PathChildrenCache(client, Workers, true)
    workersCache.getListenable.addListener(new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
        handleChildEvent(client, event, "worker", addWorkerButton, workerNumber)
      }
    })
    workersCache.start()

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

    taskButtonClicks.subscribe { _ =>
      val nextNumber = taskNumber.incrementAndGet()

      val path = s"$Tasks/task-$nextNumber"
      createPath("task", path, nextNumber)
      addTaskButton.text = s"$AddTaskText-$nextNumber"
    }

    workerButtonClicks.subscribe { _ =>
      val nextNumber = workerNumber.incrementAndGet()
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

    deleteTasksButtonClicks.subscribe { _ => deleteChildren(Tasks) }

    deleteWorkersButtonClicks.subscribe { _ => deleteChildren(Workers) }

    def deleteChildren(path: String) = {
      log.info(s"deleteChildren path [${path}]")
      val children = client.getChildren.forPath(path).asScala

      for (child <- children) {
        val childPath = s"$path/$child"
        log.info(s"Deleting [$childPath]")
        client.delete.guaranteed.forPath(childPath)
      }
    }


    deleteAssignmentsButton.clicks.map(_ => deleteAssignmentsObs).concat.observeOn(swingScheduler).subscribe {
      response =>  response match {

          case Success(results) =>
            resultsFields.text = resultsFields.text + "\nFinished"
            log.info(s"Finished")

            for (result <- results) {
              log.info(s"result [${result.getForPath}] [${result.getType}]")
              resultsFields.text =  s"${resultsFields.text}\n${result.getForPath}"
            }
          case Failure(f) =>
            val message = s"Delete failed [${f.getMessage}]"
            log.warn(message, f)
            resultsFields.text = s"${resultsFields.text}\n$message"
        }
    }

    def deleteAssignmentsObs: Observable[Try[List[CuratorTransactionResult]]] = {
      Observable.from(deleteAssignments()).timeout(1.seconds).onErrorResumeNext(t => Observable.items(Failure(t)))
    }

    def deleteAssignments(): Future[Try[List[CuratorTransactionResult]]] = {
      Future {
        Try {
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
          results.asScala.toList // Note doing toList as the Scala impl for an iterator is a stream.
        }
      }
    }

    def delete(path: String, transaction: CuratorTransaction): CuratorTransactionFinal = {
      transaction.delete.forPath(path).and()
    }
  }


  def top = new GuiFrame with GuiLogic
}

