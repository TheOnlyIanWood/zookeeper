package curator
package gui

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger
import akka.actor.{PoisonPill, Identify, ActorRef, ActorSystem}
import chapter_03.Logger
import curator.Master._
import curator.WorkerActor.{ShutdownGracefully, TaskRequest}
import org.apache.curator.framework.api.transaction.{CuratorTransactionResult, CuratorTransaction, CuratorTransactionFinal}
import org.apache.curator.framework.recipes.cache.{ChildData, PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import rx.lang.scala._
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type._
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.swing._
import scala.swing.event._
import scala.util.{Random, Try, Failure, Success}
import scala.util.control.NonFatal
import scala.concurrent.ExecutionContext.Implicits.global


object Util{

  def createPath(client:  CuratorFramework, comment: String, path: String, number: Int): Observable[Try[(String, Int)]] = {
    Observable.from(
      Future {
        Try {
            createPathSync(client,comment,path,number)
        }
      }).timeout(1.seconds).onErrorResumeNext(t => Observable.items(Failure(t)))
  }

  def createPathSync(client:  CuratorFramework, comment: String, path: String, number: Int)={
    log.info(s"going to make [$comment] [$path]")
    val result = client.create.forPath(path, number.toString.getBytes)
    (result, number)
  }

  def createPathSync(client: CuratorFramework, comment: String, path: String, data: Array[Byte]) = {
    log.info(s"going to make [$comment] [$path]")
    val result = client.create.forPath(path, data)
    (result, data)
  }

}

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
    val stopWorkersButton = new Button {      text = "Stop Workers"    }
    val clearUpWorkersButton = new Button {      text = "Clear up Workers"    }
    val resultsFields = new TextArea
    val scrollPane = new ScrollPane(resultsFields)

    contents = new BorderPanel {

      import BorderPanel.Position._

      layout(new BorderPanel {
        layout(addTaskButton) = West
        layout(addWorkerButton) = East
        layout(stopWorkersButton) = North
        layout(clearUpWorkersButton) = Center

      }) = North
      layout(scrollPane) = Center
    }
  }

  trait GuiLogic {
    self: GuiFrame =>

    import Master._
    import Util._


    private val client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 5))
    client.start()

    private val simpleDateFormatter = new ThreadLocal[SimpleDateFormat]()
    simpleDateFormatter.set(new SimpleDateFormat("HH:mm:ss.SSS"))

    private val taskNumber = new AtomicInteger(0) //using incrementAndGet so first will be 1
    private val workerNumber = new AtomicInteger(0)

    val system = ActorSystem()

    val rand = new Random(System.currentTimeMillis)

    private val workersCache = new PathChildrenCache(client, Workers, true)
    workersCache.getListenable.addListener(new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {

        val data = new String(event.getData.getData)
        event.getType match {
          case CHILD_ADDED =>
            log.info(s"ADDED: data [${data}] from event [$event]")
            system.actorSelection(data) ! "hi"

          case CHILD_REMOVED =>
            log.info(s"REMOVED: data [${data}] from event [$event]")

        }

        // Hector showed me.
//        system.actorSelection(path) ! Identify(path)

      }
    })
    workersCache.start()


    stopWorkersButton.reactions += {
      case ButtonClicked(_) =>
        log.info("delete workers")
        val workersList: List[ChildData] = workersCache.getCurrentData.asScala.toList
        for(w <- workersList){
          val actor = new String(w.getData)
          log.info(s"Asking to shutdown [${actor}]")
          system.actorSelection(actor) ! ShutdownGracefully
        }


    }

    addTaskButton.reactions += {
      case ButtonClicked(_) =>
        val nextNumber = taskNumber.incrementAndGet()
        val workersList = workersCache.getCurrentData


        // TODO use some sort of load balancing
        // http://doc.akka.io/docs/akka/snapshot/scala/routing.html

        if (!workersList.isEmpty) {
          val designatedWorker = new String(workersList.get(rand.nextInt(workersList.size)).getData)
          system.actorSelection(designatedWorker) ! TaskRequest(nextNumber)
          addTaskButton.text = s"$AddTaskText-$nextNumber"
        }

    }

    addWorkerButton.reactions += {
      case ButtonClicked(_) =>
        val nextNumber = workerNumber.incrementAndGet()
        val workerName = s"worker-$nextNumber"
        val path = s"$Workers/$workerName"
        val newActor = system.actorOf(WorkerActor.props(workerName, path, nextNumber), workerName)
        newActor ! "hello"
        addWorkerButton.text = s"$AddWorkerText-$nextNumber"
    }

    clearUpWorkersButton.clicks.map(_ => clearUpWorkersFutureObs).concat.observeOn(swingScheduler).subscribe {
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
    

    def clearUpWorkersFutureObs: Observable[Try[List[CuratorTransactionResult]]] = {
      Observable.from(clearUpWorkersFuture()).timeout(1.seconds).onErrorResumeNext(t => Observable.items(Failure(t)))
    }

    def clearUpWorkersFuture(): Future[Try[List[CuratorTransactionResult]]] = {
      Future {
        Try {
          val transaction = client.inTransaction()
          val workers = client.getChildren.forPath(Workers).asScala.map{
            worker => s"$Workers/$worker"
          }
          log.info(s"workers [${workers}]")

          for {
            worker <- workers
             } {
            log.info(s"deleting worker [$worker]")
            if (worker == s"$Workers/worker-5") {
              throw new Exception("Hopefully fail transaction")
            }
            delete(worker, transaction)
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

