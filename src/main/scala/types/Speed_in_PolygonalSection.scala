
package src.main.scala.types

import com.vividsolutions._
import org.geoscript.geometry._

/* This case-class represents a speed in a geometry of New York
 * City, as returned from its OpenData URL
 *
 * In a similar sense, it is a simplification of the WEKA instance
 * that represents that speed, and it is smaller to fit into RAM
 */

case class Speed_in_PolygonalSection(speed: Double,
                                     geometry: jts.geom.Geometry,
                                     polygon_encoded: String,
                                     centroid: Point,
                                     well_known_addr: String) {

        /*
         * Handy alias to access the "geometry: jts.geom.Geometry" field,
         * by alias "geom"
         */

        val geom = geometry

}

