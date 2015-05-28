package curator
package gui

/**
 * Created by ian on 28/05/15.
 */
object SimpleGui  extends scala.swing.SimpleSwingApplication {
  import rx.lang.scala._

  import scala.swing._
  import scala.swing.event._

  def top = new MainFrame {
    title = "Swing Observables"

    val button = new Button {
      text = "Click"
    }

    contents = button

    val buttonClicks = Observable[Unit] { sub =>
      button.reactions += {
        case ButtonClicked(_) => sub.onNext(())
      }
    }

    buttonClicks.subscribe(_ => log("button clicked"))
  }

}
