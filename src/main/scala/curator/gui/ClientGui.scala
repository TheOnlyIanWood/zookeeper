package curator
package gui

import java.text.SimpleDateFormat
import java.util
import java.util.Date
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

    case class ButtonTask(button: Button, task: String)
    val AddTaskText = "Add Task"
    val AddWorkerText = "Add Worker"

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

    private val simpleDateFormatter = new ThreadLocal[SimpleDateFormat]()
    simpleDateFormatter.set(new SimpleDateFormat("HH:mm:ss.SSS"))

    private val taskNumber = new AtomicInteger(0) //using incrementAndGet so first will be 1
    private val workerNumber = new AtomicInteger(0)

    def handleChildEvent(client: CuratorFramework,
                         event: PathChildrenCacheEvent,
                         entityType: String,
                         button: Button,
                         buttonText: String,
                         ai: AtomicInteger) = {

      val path = event.getData.getPath

      val number = path.split("-")(1).toInt

      val eventType = event.getType
      val eventDescription = s"path [${path}] type [${eventType}] Number [$number]"
      log.info(eventDescription)
      eventType match {
        case CHILD_ADDED => if (number > ai.get()) {
          log.info(s"Setting $entityType number to [${number}]")
          ai.set(number)
          button.text = s"$buttonText-$number" // TODO this is not on AWT thread.
        }
        case _ => log.info(s"not handling [${eventDescription}]")

      }
    }

    private val tasksCache = new PathChildrenCache(client, Tasks, true)
    tasksCache.getListenable.addListener(new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
        handleChildEvent(client, event, "task", addTaskButton, AddTaskText, taskNumber)
      }
    })
    tasksCache.start()

    private val workersCache = new PathChildrenCache(client, Workers, true)
    workersCache.getListenable.addListener(new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
        handleChildEvent(client, event, "worker", addWorkerButton, AddWorkerText, workerNumber)
      }
    })
    workersCache.start()

    val deleteTasks = ButtonTask(deleteTasksButton, Tasks)
    val deleteWorkers = ButtonTask(deleteWorkersButton, Workers)

    wireDeleteButton(deleteTasks)
    wireDeleteButton(deleteWorkers)

    def wireDeleteButton(b: ButtonTask) = {
      b.button.clicks.map(_ => deleteChildren(b.task)).concat.observeOn(swingScheduler).subscribe { _ match {
          case Success(results) =>
            val message = s"Finished deleting ${b.task}"
            appendResults(message)
            log.info(message)

            for (result <- results) {
              val resultDescription = s"[${result.getForPath}] deleted"
              log.info(resultDescription)
              appendResults(resultDescription)
            }
          case Failure(t) => log.warn(s"delete failed [${t}]")
        }
      }
    }

    addTaskButton.clicks.map { _ =>
      val nextNumber = taskNumber.incrementAndGet()
      val path = s"$Tasks/task-$nextNumber"
      createPath("task", path, nextNumber)
    }.concat.observeOn(swingScheduler).subscribe { response => response match {
      case Success(results) =>
        val (result, number) = results
        log.info(s"results [${results}]. [$result] [$number]")
        addTaskButton.text = s"$AddTaskText-$number"
        appendResults(s"[$result] created.")
      case Failure(t) => log.info(s"t [${t}]")
    }

    }

    addWorkerButton.clicks.map { _ =>
      val nextNumber = workerNumber.incrementAndGet()
      val path = s"$Workers/worker-$nextNumber"
      createPath("worker", path, nextNumber)
      }.concat.observeOn(swingScheduler).subscribe { response => response match {
          case Success(results) =>
            val (result, number) = results
            log.info(s"results [${results}]. [$result] [$number]")
            addWorkerButton.text = s"$AddWorkerText-$number"
            appendResults(s"[$result] created.")
          case Failure(t) => log.info(s"t [${t}]")
         }
    }

    private def createPath(comment: String, path: String, number: Int): Observable[Try[(String, Int)]] = {
      Observable.from(
          Future {
            Try {
              log.info(s"going to make [$comment] [$path]")
              val result = client.create.forPath(path, number.toString.getBytes)
              (result, number)
            }
          }).timeout(1.seconds).onErrorResumeNext(t => Observable.items(Failure(t)))
    }

    def deleteChildren(path: String): Observable[Try[List[CuratorTransactionResult]]] = {
      Observable.from(
      Future {
        Try {
          log.info(s"deleteChildren path [${path}]")
          val transaction = client.inTransaction()
          val children = client.getChildren.forPath(path).asScala

          for (child <- children) {
            val childPath = s"$path/$child"
            log.info(s"Deleting [$childPath]")
            transaction.delete.forPath(childPath)
          }
          val results: util.Collection[CuratorTransactionResult] = transaction.asInstanceOf[CuratorTransactionFinal].commit()
          results.asScala.toList // Note doing toList as the Scala impl for an iterator is a stream.
        }
      }).timeout(1.seconds).onErrorResumeNext(t => Observable.items(Failure(t)))
    }

    deleteAssignmentsButton.clicks.map(_ => deleteAssignmentsObs).concat.observeOn(swingScheduler).subscribe {
      response =>  response match {

          case Success(results) =>
            appendResults(s"Finished")
            log.info(s"Finished")

            for (result <- results) {
              log.info(s"result [${result.getForPath}] [${result.getType}] deleted")
              appendResults(result.getForPath)
            }
          case Failure(f) =>
            val message = s"Delete failed [${f.getMessage}]"
            log.warn(message, f)
            appendResults(message)
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

    private def appendResults(result: String)={
      val datePart = simpleDateFormatter.get.format(new Date())
      resultsFields.text =  s"${resultsFields.text}\n$datePart\t$result"
    }

  }


  def top = new GuiFrame with GuiLogic
}

