package curator
package gui

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger
import akka.actor.Actor.Receive
import akka.actor._
import akka.event.Logging
import chapter_03.Logger
import curator.Master._
import curator.WorkerActor.{ShutdownGracefully, TaskRequest}
import curator.WorkerType.{Small, Big, WorkerType}
import curator.gui.MasterActor.{CreatedWorker, CreateWorker}
import org.apache.curator.framework.api.transaction.{CuratorTransactionResult, CuratorTransaction, CuratorTransactionFinal}
import org.apache.curator.framework.recipes.cache._
import org.apache.curator.framework.state.{ConnectionStateListener, ConnectionState}
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
    if (log.isDebugEnabled) {
      log.debug(s"going to make [$comment] [$path]")
    }
    val result = client.create.forPath(path, number.toString.getBytes)
    (result, number)
  }

  def createPathSync(client: CuratorFramework, comment: String, path: String, data: Array[Byte]) = {
    if (log.isDebugEnabled) {
      log.debug(s"going to make [$comment] [$path]")
    }
    val result = client.create.forPath(path, data)
    (result, data)
  }

}

/**
 * Created by ian on 28/05/15.
 */
object ClientGui extends scala.swing.SimpleSwingApplication  {

  abstract class GuiFrame extends MainFrame {
    title = "Client Gui"

    case class ButtonTask(button: Button, task: String)
    val AddTaskText = "Add Task"
    val AddBigWorkerText = s"Add $Big Worker"
    val AddSmallWorkerText = s"Add $Small Worker"

    val addTaskButton = new Button {      text = AddTaskText    }
    val addBigWorkerButton = new Button {     text =AddBigWorkerText   }
    val addSmallWorkerButton = new Button {     text = AddSmallWorkerText   }
    val stopWorkersButton = new Button {      text = "Stop Workers"    }
    val clearUpWorkersButton = new Button {      text = "Clear up Workers"    }
    val broadcastButton = new Button { text = "Ask all workers"}
    val printWorkers = new Button { text = "Print workers"}
    val resultsFields = new TextArea
    val scrollPane = new ScrollPane(resultsFields)

    contents = new BorderPanel {

      import BorderPanel.Position._

      layout(new BorderPanel {
        layout(addBigWorkerButton) = East
        layout(addSmallWorkerButton) = West
        layout(stopWorkersButton) = North
        layout(clearUpWorkersButton) = South

      }) = North

      layout(new FlowPanel(addTaskButton, printWorkers, broadcastButton)) = South
      layout(scrollPane) = Center
    }
  }

  trait GuiLogic extends ConnectionStateListener {
    needs: GuiFrame =>

    private val client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 5))
    client.getConnectionStateListenable.addListener(this)
    client.start()

    private val simpleDateFormatter = new ThreadLocal[SimpleDateFormat]()
    simpleDateFormatter.set(new SimpleDateFormat("HH:mm:ss.SSS"))

    private val taskNumber = new AtomicInteger(0) //using incrementAndGet so first will be 1
    private val workerNumber = new AtomicInteger(0)

    val system = ActorSystem()
    val Master = "master"
    val masterActor = system.actorOf(MasterActor.props, Master)

    val rand = new Random(System.currentTimeMillis)

    private val workersCacheBig = new PathChildrenCache(client, s"$Workers/$Big", true)
    workersCacheBig.getListenable.addListener(new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {

        val data = new String(event.getData.getData)
        event.getType match {
          case CHILD_ADDED =>
            log.info(s"ADDED: data [${data}] from event [$event]")
            val a: ActorSelection = system.actorSelection(data)
            a ! "hi"

          case CHILD_REMOVED =>
            log.info(s"REMOVED: data [${data}] from event [$event]")

        }

        // Hector showed me.
//        system.actorSelection(path) ! Identify(path)

      }
    })
    workersCacheBig.start()

    private val workersTreeCache = new TreeCache(client, Workers)
    workersTreeCache.getListenable.addListener( new TreeCacheListener {
      override def childEvent(client: CuratorFramework, event: TreeCacheEvent): Unit = {

        val path = event.getData.getPath
        log.info(s"workersTreeCache [${path}] type [${event.getType}]")
      }
    }
    )
    workersTreeCache.start()


    stopWorkersButton.reactions += {
      case ButtonClicked(_) =>
        log.info("Stop workers")
        val workersList: List[ChildData] = workersCacheBig.getCurrentData.asScala.toList
        for(w <- workersList){
          val actor = new String(w.getData)
          log.info(s"Asking to shutdown [${actor}]")
          system.actorSelection(actor) ! ShutdownGracefully
        }


    }

    addTaskButton.reactions += {
      case ButtonClicked(_) =>
        val nextNumber = taskNumber.incrementAndGet()
        val workersList = workersCacheBig.getCurrentData

//        log.info(s"workersList [${workersList}]")

        // TODO use some sort of load balancing
        // http://doc.akka.io/docs/akka/snapshot/scala/routing.html

        if (!workersList.isEmpty) {
          val designatedWorker = new String(workersList.get(rand.nextInt(workersList.size)).getData)
          system.actorSelection(designatedWorker) ! TaskRequest(nextNumber)
          addTaskButton.text = s"$AddTaskText-$nextNumber"
        }else{
          log.info("No workers found")
        }

    }

    broadcastButton.reactions += {
      case ButtonClicked(_) =>
        val nextNumber = taskNumber.incrementAndGet()
        system.actorSelection(s"akka://default/user/$Master/*") ! TaskRequest(nextNumber)
    }

    printWorkers.reactions += {
      case ButtonClicked(_)=>
        val map = workersTreeCache.getCurrentChildren(Workers)
        appendResults(map.toString)
    }

    addBigWorkerButton.reactions += {
      case ButtonClicked(_) =>
        createWorker(WorkerType.Big, addBigWorkerButton, AddBigWorkerText)
    }
    addSmallWorkerButton.reactions += {
      case ButtonClicked(_) =>
        createWorker(WorkerType.Small, addSmallWorkerButton, AddSmallWorkerText)
    }

    def createWorker(workerType: WorkerType, button: Button, text: String)={
      val nextNumber = workerNumber.incrementAndGet()
      val workerName = s"worker-$nextNumber"
      val path = s"$Workers/$workerType/$workerName"
//      val newActor = system.actorOf(WorkerActor.props(workerName, path, nextNumber,workerType), workerName)
      
      masterActor ! CreateWorker(workerName, path, nextNumber,workerType)

//      newActor ! "hello"
      button.text = s"$text-$nextNumber"
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

          val workers = for {
            workerType <- WorkerType.workerTypes
            worker <- client.getChildren.forPath(s"$Workers/$workerType").asScala
          } yield  s"$Workers/$workerType/$worker"

          log.info(s"workers [${workers}]")

          for {
            worker <- workers
             } {
            log.info(s"deleting worker [$worker]")
            if (worker == s"$Workers/$Big/worker-5") { // TODO this will need to change too.
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

    override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
      log.info(s"newState [${newState}]")
    }

  }


  def top = new GuiFrame with GuiLogic

}


object MasterActor {

  sealed trait MasterActorMessage
  case class CreateWorker(name: String, path: String, number: Int, workerType: WorkerType) extends MasterActorMessage
  case object CreatedWorker extends MasterActorMessage

  def props: Props = Props(new MasterActor)
}

class MasterActor extends Actor {
  val log = Logging(context.system, this)

  override def receive: Actor.Receive = {

    case CreateWorker(name, path, number, workerType) =>
      val newActor = context.actorOf(WorkerActor.props(path, number,workerType), name)

      newActor ! "hello"
    case CreatedWorker =>
      log.info(s"Created [${sender.path.name}]")

    case msg => log.info(s"msg [${msg}]")
  }



}

/**
  Demo
./shutDown, clearData
startClient, exhibitor
master
clientGui
addBig worker
clearup with worker-5 there - show the delete of paths not work
remove him manually then try again.
stop and start again.
  */