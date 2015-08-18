
package src.main.scala.utils.conversion


import scala.util.control.Exception._
import com.vividsolutions._
import org.geoscript.geometry.io._
import org.geoscript.geometry._

import src.main.scala.logging.Logging._

object String_to_Geom {

    /*
     * method: convert
     *
     * Converts a string with a sequence of coordinates of the form:
     *
     *      X1 Y1,X2 Y2,...,X[n] Y[n]
     *
     * where each pair "X[i] Y[i]" is separated from the previous and next
     * pairs by a comma ",", in to Geometry.
     *
     * @param in_s the input string with the csv of the coordinates
     * @return the geome
     */
    
    def convert(in_s: String): jts.geom.Geometry =
    {
          val coords = catching(classOf[java.lang.RuntimeException]) opt
                         in_s.split("\\s+").map {
                                    case point_str: String => {
                                        val coord = point_str.split(",")
                                        (coord(0).toDouble, coord(1).toDouble)
                                    }
                             }
    
          if (coords != null && coords.isDefined && coords.get.length > 0)  {
              // It is a valid set of coordinates
              // call GeoScript
              val geometry_instance = builder.LineString(coords.get)
              log_msg(DEBUG, "Geometry instance is " + geometry_instance)
              return geometry_instance
          } else
              return null
    } // method convert

    /*
     * method: apply
     *
     * Make the implicit method ("apply(...)") in Scala an alias to
     * convert(s) for this utility object "String_to_Geom"
     */

    def apply(in_s: String): jts.geom.Geometry = convert(in_s)

} // object String_to_Geom


