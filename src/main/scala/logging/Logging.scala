package src.main.scala.logging

/*
 * Object: Logging
 *
 * Handle logging messages to stderr with its method log_msg(...)
 */

object Logging extends Enumeration {
  type Logging = Value

  val EMERGENCY = Value(0)
  val ALERT = Value(1)
  val CRITICAL = Value(2)
  val ERROR = Value(3)
  val WARNING = Value(4)
  val NOTICE = Value(5)
  val INFO = Value(6)
  val DEBUG = Value(7)

  var loggingThreshold: Logging = ERROR

  def log_msg(level: Logging, err_msg: String)
  {
    if (level <= loggingThreshold) {
      System.err.println(level.toString + ": " + err_msg)
    }
  }
}

