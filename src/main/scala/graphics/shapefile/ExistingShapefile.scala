
package src.main.scala.graphics.shapefile


import scala.collection.JavaConversions._
import scala.collection.mutable._
import scala.util.control.Breaks._

import org.geoscript.feature._
import org.geoscript.layer._
import com.vividsolutions._
import org.geotools.feature.DefaultFeatureCollection


// The following import is for filtering the features in the ESRI Shapefile
// according to the NYC Real-time Speed

import src.main.scala.types.Speed_in_PolygonalSection



class ExistingShapefile(
     val shp_filename: String
   ) {
   
   /*
    * Use only one layer since the New York City's Department of City Planning
    * LION Single-Line Street Geo-database has only one layer of features.
    */
   
   var layer: Layer = null


   /*
    * method: open
    *
    * Uses GeoTools to open a shapefile, given by the filename in the field
    * "shp_filename". 
    *
    * Sets the resulting layer in the mutable field "layer".
    *
    */

   def open()
   {
       // Open the filename as a ESRI Shapefile
   
       val shp = Shapefile(shp_filename)
   
       // println("DEBUG: Class of ShpFile: " + shp.getClass)
   
       // println("DEBUG: Number of items in the shapefile: " + shp.count)
   
       // println("DEBUG: Schema: " + shp.schema)
   
       // println("DEBUG: Features: " + shp.features)
       // println("DEBUG: Bounds rectangle: " + shp.features.getBounds())
   
       layer = shp
   }


   /*
    * method: filter_geom_features
    *
    * Uses GeoTools to create a new collection of features given the current
    * collection of features in the field "layer" and a filtering set of
    * geometries with which to select which existing features of "layer"
    * should go to the new collection of features.
    *
    * @param filtering_geom the list of filtering geometries to match
    *                       (at least one matching geometry is enough for a
    *                        positive match, ie., it is an OR-ed by this list
    *                        of filtering geometries to match)
    * @param resulting_features the feature collection where to save the
    *                           filtered results
    */
   
   def filter_geom_features(filtering_geoms: List[Speed_in_PolygonalSection],
                            resulting_features: DefaultFeatureCollection)
      {
          /* TODO: this has to be rewritten to use instead the
           *       subCollection(Filter) method of
           *       "input_features: FeatureCollection", instead of the full
           *       for-loop in this initial test version
           */

          for (f <- layer.features.toArray()) {

                val feature =
               f.asInstanceOf[org.geotools.feature.simple.SimpleFeatureImpl]

                var this_feature_is_selected: Boolean = false
      
                // See if this feature is contained in one of the
                // filtering_geoms
                for (constr <- filtering_geoms) { 
                    // this feature's geometry intersects this filter?
                    val feat_geom: jts.geom.Geometry =
                                    feature.getAttribute("the_geom").
                                               asInstanceOf[jts.geom.Geometry]

                    this_feature_is_selected = constr.geom.intersects(feat_geom)
                    if (!this_feature_is_selected) {
                       /*
                        * Try if the constraint is a polygon and if this feature's
                        * geometry is contained in it.
                        * The most general -and optimum- way to handle this is
                        * to use:
                        *      IntersectionMatrix
                        *             filtering_geom.relate(feat_geom)
                        * and see what are the relations between both geometries
                        * http://www.vividsolutions.com/jts/javadoc/com/vividsolutions/jts/geom/Geometry.html#relate(com.vividsolutions.jts.geom.Geometry)
                       */
                       if (constr.geom.isInstanceOf[jts.geom.Polygon]) {
                            this_feature_is_selected =
                                          constr.geom.contains(feat_geom)
                       }
                    }

                    // If this geometrical constraint matched this feature, we
                    // don't need to check the rest of geometrical constraints
                    // on this feature.

                    if (this_feature_is_selected) break
                }
      
                if (this_feature_is_selected) {
                    // Add this feature to a resulting feature collection
                    resulting_features.add(feature)
                }
          }
      } // end of method filter_geom_features

}

