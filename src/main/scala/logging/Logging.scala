
package src.main.scala.logging

/*
 * function: log_msg
 *
 * Handle logging error messages
 *
 * Better to do with an error-level, like in Unix (ie., Debug, Info, Notice,
 * Warning, etc), and this is why we don't use a Boolean value here, but an
 * integer representing the logging-level threshold.
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
         if(level <= loggingThreshold) {
             System.err.println(level.toString + ": " + err_msg)
         }
     }
}

