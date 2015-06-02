import java.awt.event.MouseAdapter
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.Executor

import rx.lang.scala.{Scheduler, Observable}

import scala.swing.{Table, TextField, Button}
import scala.swing.event.{ValueChanged, ButtonClicked}

package object curator {

  val formatter = new SimpleDateFormat("HH:mm:ss.SSS")

  def log(msg: String) {
    println(s"${formatter.format(new Date())} ${Thread.currentThread.getName}: \t$msg")
  }


  //copied from ch6 concurrency examples.
  implicit class ButtonOps(val self: Button) {
    def clicks = Observable[Unit] { sub =>
      self.reactions += {
        case ButtonClicked(_) => sub.onNext(())
      }
    }
  }

  implicit class TextFieldOps(val self: TextField) {
    def texts = Observable[String] { sub =>
      self.reactions += {
        case ValueChanged(_) => sub.onNext(self.text)
      }
    }
  }

  implicit class TableOps(val self: Table) {
    def rowDoubleClicks = Observable[Int] { sub =>
      self.peer.addMouseListener(new MouseAdapter {
        override def mouseClicked(e: java.awt.event.MouseEvent) {
          if (e.getClickCount == 2) {
            val row = self.peer.getSelectedRow
            sub.onNext(row)
          }
        }
      })
    }
  }

  def swing(body: =>Unit) = {
    val r = new Runnable {
      def run() = body
    }
    javax.swing.SwingUtilities.invokeLater(r)
  }

  val swingScheduler = new Scheduler {
    val asJavaScheduler = rx.schedulers.Schedulers.from(new Executor {
      def execute(r: Runnable) = javax.swing.SwingUtilities.invokeLater(r)
    })
  }


}

