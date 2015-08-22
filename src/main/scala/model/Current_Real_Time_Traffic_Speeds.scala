package src.main.scala.model


import scala.collection.mutable.ListBuffer

import _root_.java.io.File
import _root_.java.net.URLEncoder

import weka.core.converters.SerializedInstancesLoader

import org.geoscript.geometry._

import src.main.scala.logging.Logging._
import src.main.scala.config.Config
import src.main.scala.utils.download.Download_URL_to_File
import src.main.scala.types.Speed_in_PolygonalSection
import src.main.scala.utils.conversion.String_to_Geom


class Current_Real_Time_Traffic_Speeds(
                             val weka_bsi_fname: String
                        )
{

    var speeds_in_subpolygons: List[Speed_in_PolygonalSection] = null

    /*
     * method: print_polygonal_zone_in_NYC
     *
     * Prints a polygonal zone in NYC.
     * Currently it uses the Google Maps, but its purpose is to use the New
     * York City Open Data shapefiles, from GIS. (Like the Dept of City
     * Planning LION Single-Line Street Segments File GeoDB, or, e.g.,
     *
     *  https://data.cityofnewyork.us/Housing-Development/Building-Footprints/tb92-6tj8
     *
     * although this one has too much information, since it even has the
     * building foot-prints.) When the code use shapefiles instead of Google
     * Maps, its "color" parameter below will be substituted by a GeoTools'
     * "Style" object, which is richer.
     */

    protected def print_polygonal_zone_in_NYC(
                                  polyg_zone: Speed_in_PolygonalSection,
                                  color: String, dest_png: String
                          )
    {

          // URL-encode the encoded polygonal zone in var "encoded_polyline"

          val enc_polyl = polyg_zone.polygon_encoded
          val url_polygon: String = URLEncoder.encode(enc_polyl, "UTF-8").
                                               replace("+", "%20")

          // Print the URL in Google Maps for it
          val centr_longitude = polyg_zone.centroid.x
          val centr_latitude = polyg_zone.centroid.y
          val gmap_url = "https://maps.googleapis.com/maps/api/staticmap?" +
                        f"center=$centr_longitude%6.6f,$centr_latitude%6.6f" +
                        "&zoom=14&size=800x800&maptype=roadmap&scale=2" +
                        f"&path=fillcolor:0xFF$color" +
                        "%7Ccolor:0xFFFFFF00%7Cenc:" +
                        url_polygon
          log_msg(DEBUG, "Google Maps URL is " + gmap_url)

          val gmap_png = new File(dest_png)
          Download_URL_to_File(gmap_url, gmap_png)
    } // print_polygonal_zone_in_NYC()


    /*
     * method: print_speed_vibrancy_map
     *
     * Prints a map with all the polygonal sub-sections of New York City
     * in different colors according to the current real-time speed in
     * its polygonal sub-sections
     *
     * It receives the list of polygons (: List[Speed_in_PolygonalSection])
     * and also the min_speed among that list, because that polygonal zone(s)
     * with currently the slowest speed in traffic must be filled in red, and
     * not that zone with traffic speed 0.0, which is pbby won't appear.
     *
     * Similar as function above, this should use GIS from Open Data as not
     * to require API registration keys as Google Maps. The same issue occurs
     * with Open Street Maps, it also requires an API key to support multiple
     * layers of visualization (the polygonal zones of traffic in NYC), but
     * we want to run without API keys:
     *
     *    http://wiki.openstreetmap.org/wiki/Static_map_images
     *
     *    http://wiki.openstreetmap.org/wiki/Elements
     *
     * If the API Key were available from the end-user (e.g., if he/she has
     * an API Key to query Google Maps for multiple layers), then this data
     * exploration project of the traffic would have been done instead in
     * IPython Notepad and Pandas using the Google Maps Python libraries and
     * the "osgeo.gdal" GeoSpatial Data Abstraction Library:
     *
     *   https://developers.google.com/api-client-library/python/apis/mapsengine/v1
     *
     *   http://www.osgeo.org/gdal_ogr
     *
     * and the "python-weka-wrapper.jar" to use WEKA in Python
     */

    protected def print_speed_vibrancy_map(min_speed: Double,
                                   polygons: List[Speed_in_PolygonalSection],
                                   dest_png: String
                           )
    {
          var gmap_paths = new StringBuilder(32*1024)

          /* We order the polygonal zones by speed and take the slowest 12
           * zones because Google Maps would not return more than 12
           * polylines, ie., with 14 polylines it may succeed or it may fail
           */
          for (polygon_zone <- polygons.sortBy(_.speed).take(12)) {
               val polyg: String = URLEncoder.encode(
                                        polygon_zone.polygon_encoded, "UTF-8"
                                     ).replace("+", "%20")
               // Find which color according to the traffic speed to print
               // this zone in New York City: green is the best, red is slowest
               var color : Long = 0x00FF00
               if (polygon_zone.speed >= Config.target_optimum_NYC_speed)
                     color = 0x00FF00
               else {
                     val delta_speed = (polygon_zone.speed - min_speed) /
                                (Config.target_optimum_NYC_speed - min_speed)

                     val delta_color: Int = 0x00FF00 - 0x0000FF
                     color = 0x0000FF +
                                   scala.math.round(delta_color * delta_speed)
               }

               val color_str = f"$color%06X"
               gmap_paths.append("&path=fillcolor:0x" + color_str +
                                 "%7Ccolor:0xFFFFFF00%7C" + "enc:" + polyg)
          }

                         // "center=42.1497,-74.9384&zoom=10&size=800x800"

          val gmap_url = "https://maps.googleapis.com/maps/api/staticmap?" +
                         // "center=40.7836,-73.9625&zoom=11&size=800x800" +
                         "size=800x800&scale=2&maptype=roadmap" +
                         gmap_paths.toString

          val gmap_png = new File(dest_png)
          Download_URL_to_File(gmap_url, gmap_png)
    } // print_speed_vibrancy_map()


    /*
     * method: load_traffic_speed_per_polygon_in_the_city
     *
     * Loads a WEKA SerializedInstances file with the real-time traffic in
     * NYC and filters those records which have valid polygonal sections for
     * GeoScript.
     * Note: the NYC traffic has the polygons as poly-lines and also in
     * encoded format:
     * https://developers.google.com/maps/documentation/utilities/polylinealgorithm
     *
     * The first format of the polygon is used to validate it, the second to
     * print it in Google Maps.
     *
     * @param weka_bsi_file the filename of the WEKA binary serialized
     *                      instances to load
     * @return the loaded list of polygons real-time speed
     */

    def load_traffic_speed_per_polygon_in_the_city()
                                : List[Speed_in_PolygonalSection] =
    {
        // load the New York City Real-Time traffic speed loaded from
        //     http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt
        // into GeoScript/Java Topology Suite polygonal sections of New York

        var sinst_in: SerializedInstancesLoader =
                                              new SerializedInstancesLoader()
        sinst_in.setSource(new File(weka_bsi_fname));

        // Get list of valid polygons (OpenData also gives invalid ones)
        var current_valid_polygons =
                                  new ListBuffer[Speed_in_PolygonalSection]()

        // Record the two locations which have the slowest and fastest speeds
        var min_speed_zone = new Speed_in_PolygonalSection(999999.0, null,
                                                           "", null, "")

        var max_speed_zone = new Speed_in_PolygonalSection(-1.0, null,
                                                           "", null, "")

        for (instance <- Iterator.continually(sinst_in.getNextInstance(null)).
                                     takeWhile(_ != null)) {

             // Split the fields of this WEKA instance:
             //  The 7th column is the polygonal section inside New York City
             val polygon_str = instance.stringValue(6)
             //       The 8th column in the WEKA instance is the
             //        encoded-polygonal by the Google polyline algorithm
             //        format, below:
             // https://developers.google.com/maps/documentation/utilities/polylinealgorithm
             val polygon_enc = instance.stringValue(7)
             // The 2nd column is the traffic speed in that polygonal section
             val speed = instance.value(1)
             //       The 13th column is a well-known address inside New York
             val well_known_address = instance.stringValue(12)

             log_msg(DEBUG, "speed is: " + speed +
                       "\n     inside polygonal section " + polygon_str +
                       "\n     reference point: '" + well_known_address + "'")

             val geometry_instance = String_to_Geom(polygon_str)

             if (geometry_instance != null)  {
                     // Find the centroid point of this geometry
                     val centroid = geometry_instance.centroid
                     log_msg(DEBUG, "Centroid is " + centroid)
                     current_valid_polygons += new Speed_in_PolygonalSection(
                                                          speed,
                                                          geometry_instance,
                                                          polygon_enc,
                                                          centroid,
                                                          well_known_address)

                     // See if this is the location with the less speed so far
                     if(speed < min_speed_zone.speed) {
                           min_speed_zone = new Speed_in_PolygonalSection(
                                                          speed,
                                                          geometry_instance,
                                                          polygon_enc,
                                                          centroid,
                                                          well_known_address)
                     }
                     if(speed > max_speed_zone.speed) {
                           max_speed_zone = new Speed_in_PolygonalSection(
                                                          speed,
                                                          geometry_instance,
                                                          polygon_enc,
                                                          centroid,
                                                          well_known_address)
                     }
             } else
                log_msg(WARNING, "ignoring polygon around reference address: '"
                          + well_known_address +
                          "'\n   not-parseable poly-points: " + polygon_str)

         }
         // Print those polygonal subsections of New York City that happen to
         // have now the slowest and the fastest speeds
         if ( min_speed_zone.polygon_encoded != "" ) {
               print_polygonal_zone_in_NYC(min_speed_zone,
                                           "00FFFF",
                                           "visualization_slowest_speed.png")
               println("Map of zone with the slowest speed at this moment: " +
                       min_speed_zone.speed +
                       " (" + min_speed_zone.well_known_addr + ")\n" +
                       "Downloaded to " + "visualization_slowest_speed.png")
         }
         if ( max_speed_zone.polygon_encoded != "" ) {
               print_polygonal_zone_in_NYC(max_speed_zone,
                                           "FF00FF",
                                           "visualization_fastest_speed.png")
               println("Map of zone with the fastest speed at this moment: " +
                       max_speed_zone.speed +
                       " (" + max_speed_zone.well_known_addr + ")\n" +
                       "Downloaded to " + "visualization_fastest_speed.png")
         }
         if ( min_speed_zone.polygon_encoded != "" ) {
    
	       val list_valid_polyg = current_valid_polygons.toList

               print_speed_vibrancy_map(min_speed_zone.speed,
                                        list_valid_polyg,
                                        "visualization_vibrancy_speed.png")
               println("Map of vibrancy at this moment downloaded to: " +
                       "visualization_vibrancy_speed.png")

               speeds_in_subpolygons = list_valid_polyg
               return list_valid_polyg
         }
         return null
    }  // load_traffic_speed_per_polygon_in_the_city()


    /*
     * method: find_time_to_go_from_one_polygon_to_another
     *
     * Find the time to go from one polygon to another, using the JTS
     * distance between these polygons and the speed in the src polygon.
     * Then:
     *       time = ( distance-between-polygons / speed-in-src-polygon )
     *
     * Ie., supposing that we "annotate" by a sign certain hypothetical
     * vehicle in traffic at the source polygon, at what time does it
     * will arrive at the destination polygon.
     *
     * This method needs to be improved, since it is meaningful for the
     * traffic.
     */

    def find_time_to_go_from_one_polygon_to_another(
                                         src: Speed_in_PolygonalSection,
                                         dest: Speed_in_PolygonalSection
                                      )
    {
    }

}