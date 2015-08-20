
package src.main.scala.types

import scala.collection.mutable.ListBuffer

import com.vividsolutions._
import org.geoscript.geometry._


import org.geotools.geometry.jts.JTSFactoryFinder


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

        /* 
        protected def find_polygon(): jts.geom.Polygon = 
             if(geometry != null) {
                 val geomFactory = JTSFactoryFinder.getGeometryFactory()
     
                 var coords = geometry.getCoordinates()
                 coords.append(coords(0))
                 val hull = geomFactory.createLinearRing(coords)
                 return geomFactory.createPolygon(hull, null)
             }
        */

        val polyg: jts.geom.Polygon = {
             if (geometry != null) {
                 val coords = geometry.getCoordinates()
                 if (coords.size >= 3) {
                     /* If it has more than three coordinates, then it can
                      * be made a polygon by adding the first coordinate
                      * again as the last one, ie., by closing the coords
                      */
                     val geomFactory = JTSFactoryFinder.getGeometryFactory()

                     var hull_coords = new ListBuffer[jts.geom.Coordinate]()
                     hull_coords ++= coords

                     // append first coord at the end, closing the coords
                     hull_coords += hull_coords(0)
                     val hull =
                            geomFactory.createLinearRing(hull_coords.toArray)
                     // create the polygon
                     geomFactory.createPolygon(hull, null)
                 } else
                     null
             } else
                 null
        }
}

