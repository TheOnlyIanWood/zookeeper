import java.text.SimpleDateFormat
import java.util.Date

package object curator {

  val formatter = new SimpleDateFormat("HH:mm:ss.SSS")

  def log(msg: String) {
    println(s"${formatter.format(new Date())} ${Thread.currentThread.getName}: \t$msg")
  }

}

