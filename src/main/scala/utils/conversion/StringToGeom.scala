package src.main.scala.utils.conversion

import scala.util.control.Exception._
import com.vividsolutions._
import org.geoscript.geometry.io._
import org.geoscript.geometry._

import src.main.scala.logging.Logging._

object StringToGeom {

  /*
   * method: convertLatitLongitStr
   *
   * Converts a string with a sequence of coordinates of the form:
   *
   *      Latit1 Longit1,Latit2 Longit2,...,Latit[n] Longit[n]
   *
   * where each pair "Latit[i] Longit[i]" is separated from the previous
   * and next pairs by a comma ",", in to Geometry.
   *
   * @param in_s the input string with the csv of the coordinates
   * @return the geometry
   */

  def convertLatitLongitStr(in_s: String): jts.geom.Geometry =
  {

    val coords = catching(classOf[java.lang.RuntimeException]) opt
                   in_s.split("\\s+").map {
                              case point_str: String => {
                                  val coord = point_str.split(",")
                                  (coord(0).toDouble, coord(1).toDouble)
                                }
                     }

    if (coords != null && coords.isDefined && coords.get.length > 0) {
      // It is a valid set of coordinates
      // call GeoScript
      val geometry_instance = builder.LineString(coords.get)
      log_msg(DEBUG, "Geometry instance is " + geometry_instance)
      return geometry_instance
    } else
      return null

  } // method convertLatitLongitStr


  /*
   * method: convertLongitLatitStr
   *
   * Converts a string with a sequence of coordinates of the form:
   *
   *      Longit1 Latit1,Longit2 Latit2,...,Longit[n] Latit[n]
   *
   * where each pair "Longit[i] Latit[i]" is separated from the previous
   * and next pairs by a comma ",", in to Geometry.
   *
   * NOTE: The difference between convertLatitLongitStr(string) and
   *                              convertLongitLatitStr(string)
   *       is which comes first in the string, a latittude or a longitude
   *
   * Eg., the New York City Department of City Planning LION Single-Line
   * Street GeoDB puts "Longitude Latitude" in this order, but the NYC
   * Traffic Speed OpenData puts "Latitude Longitude" in this order.
   *
   * @param in_s the input string with the csv of the coordinates
   * @return the geometry
   */

  def convertLongitLatitStr(in_s: String): jts.geom.Geometry =
  {

    val coords = catching(classOf[java.lang.RuntimeException]) opt
                   in_s.split("\\s+").map {
                              case point_str: String => {
                                  val coord = point_str.split(",")
                                  (coord(1).toDouble, coord(0).toDouble)
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

  } // method convertLongitLatitStr

  /*
   * method: apply
   *
   * Make the implicit method ("apply(...)") in Scala an alias to
   * convert(s) for this utility object "StringToGeom"
   *
   * We prefer the order "Longitude Latitude" of the New York City
   * Department of City Planning LION Single-Line Street GeoDB.
   *
   * @param in_s the input string with the csv of the coordinates
   * @return the geometry
   */

  def apply(in_s: String, LION_order: Boolean = true): jts.geom.Geometry = {

    if (LION_order) convertLongitLatitStr(in_s)
               else convertLatitLongitStr(in_s)

  } // method apply

} // object StringToGeom

