#!/usr/bin/env scala -language:postfixOps -deprecation

import scala.App
import sys.process._
import scala.util.control.Exception._
import _root_.java.io.File
import _root_.java.net.URL
import _root_.java.net.URLEncoder
import _root_.java.text.SimpleDateFormat
import _root_.java.util.Calendar
import _root_.java.util.Date
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.math

/* Polygon */

import org.geoscript.geometry.io._
import org.geoscript.geometry._
import org.geoscript.viewer._
import com.vividsolutions._
import org.geotools.feature.DefaultFeatureCollection

/*
 * GeoScript Polygon classes, since the New York City Real-Time
 * Link Speed URL:
 *
 *    http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt
 *
 * has a column, "linkPoints", with values like:
 *
 *    "40.74047,-74.009251 40.74137,-74.00893 40.7431706,-74.008591 ..."
 *
 * which represent the speed of the traffic in that polygon in New York City.
 *
 * Since the URL represents the speeds of traffic in different polygons inside
 * New York City, and the classes above in GeoScript/JTS allow to find the
 * intersection of polygons, etc, these polygonal operations will allow us to
 * find the bottlenecks or optimum (faster) paths around different points.
 *
 * Note: this is not the Polygon of the WEKA UserClassifier, in
 *      weka/classifiers/trees/UserClassifier.java
 * but the spatial polygon of the section of speed in NYC.
 */


/* WEKA Machine Learning. Other packages and classes import other components
 * of WEKA.
 */

import weka.core.converters.SerializedInstancesLoader


/* Internal packages of this project. In this very first version, they
 * have this simple prefix, but will surely change. */

import src.main.scala.logging.Logging._

import src.main.scala.config.Config

import src.main.scala.graphics.shapefile._

import src.main.scala.types.Speed_in_PolygonalSection

import src.main.scala.utils.conversion.String_to_Geom

import src.main.scala.opendata._



/*
 * function: download_url_to_file
 *
 * Downloads an URL to a local file (the contents of the URL are not
 * interpreted, ie., they are taken as-is, as raw binary
 */

def download_url_to_file(src_url: String, dest_file: File) = {
      // From:
      // http://alvinalexander.com/scala/scala-how-to-download-url-contents-to-string-file
      //
      new URL(src_url) #> dest_file !!
}



/*
 * function: print_polygonal_zone_in_NYC
 *
 * Prints a polygonal zone in NYC.
 * Currently it uses the Google Maps, but its purpose is to use the New York
 * City Open Data shapefiles, from GIS. (Like the Dept of City Planning LION
 * Single-Line Street Segments File GeoDB, or, e.g.,
 *
 *  https://data.cityofnewyork.us/Housing-Development/Building-Footprints/tb92-6tj8
 *
 * although this one has too much information, since it even has the building
 * foot-prints.) When the code use shapefiles instead of Google Maps, its
 * "color" parameter below will be substituted by a GeoTools' "Style" object,
 * which is richer.
 */

def print_polygonal_zone_in_NYC(polyg_zone: Speed_in_PolygonalSection,
                                color: String, dest_png: String) {

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
      download_url_to_file(gmap_url, gmap_png)
}


/*
 * function: print_speed_vibrancy_map
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
 * layers of visualization (the polygonal zones of traffic in NYC), but we
 * want to run without API keys:
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

def print_speed_vibrancy_map(min_speed: Double,
                               polygons: List[Speed_in_PolygonalSection],
                               dest_png: String) {

      var gmap_paths = new StringBuilder(32*1024)

      /* We order the polygonal zones by speed and take the slowest 12 zones
       * because Google Maps would not return more than 12 polylines, ie.,
       * with 14 polylines it may succeed or it may fail
       */
      for (polygon_zone <- polygons.sortBy(_.speed).take(12)) {
           val polyg: String = URLEncoder.encode(polygon_zone.polygon_encoded,
                                                 "UTF-8").
                                          replace("+", "%20")
           // Find which color according to the traffic speed to print this
           // zone in New York City: green is the best, red is slowest
           var color : Long = 0x00FF00
           if (polygon_zone.speed >= Config.target_optimum_NYC_speed)
                 color = 0x00FF00
           else {
                 val delta_speed = ( polygon_zone.speed - min_speed ) /
                               ( Config.target_optimum_NYC_speed - min_speed )

                 val delta_color: Int = 0x00FF00 - 0x0000FF
                 color = 0x0000FF + scala.math.round(delta_color * delta_speed)
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
      download_url_to_file(gmap_url, gmap_png)
}


/*
 * function: load_NewYork_traffic_speed_per_polygon_in_the_city
 *
 * Loads a WEKA SerializedInstances file with the real-time traffic in NYC
 * and filters those records which have valid polygonal sections for
 * GeoScript.
 * Note: the NYC traffic has the polygons as poly-lines and also in encoded
 * format:
 * https://developers.google.com/maps/documentation/utilities/polylinealgorithm
 *
 * The first format of the polygon is used to validate it, the second to print
 * it in Google Maps.
 *
 * @param weka_bsi_file the filename of the WEKA binary serialized instances
 *                      to load
 * @return the loaded list of polygons real-time speed
 */

def load_NewYork_traffic_speed_per_polygon_in_the_city(weka_bsi_file: String)
     : List[Speed_in_PolygonalSection] =
{
    // load the New York City Real-Time traffic speed loaded from
    //     http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt
    // into GeoScript/Java Topology Suite polygonal sections of New York City

    var sinst_in: SerializedInstancesLoader = new SerializedInstancesLoader();
    sinst_in.setSource(new File(weka_bsi_file));

    // Get list of valid polygons (it is expected that there are invalid ones)
    var current_valid_polygons = new ListBuffer[Speed_in_PolygonalSection]()

    // Record the two locations which have the slowest and fastest speeds
    var min_speed_zone = new Speed_in_PolygonalSection(999999.0, null,
                                                       "", null, "")

    var max_speed_zone = new Speed_in_PolygonalSection(-1.0, null,
                                                       "", null, "")

    for (instance <- Iterator.continually(sinst_in.getNextInstance(null)).
                                 takeWhile(_ != null)) {

         // Split the fields of this WEKA instance:
         //       The 7th column is the polygonal section inside New York City
         val polygon_str = instance.stringValue(6)
         //       The 8th column in the WEKA instance is the encoded-polygonal
         //        by the Google polyline algorithm format, below:
         // https://developers.google.com/maps/documentation/utilities/polylinealgorithm
         val polygon_enc = instance.stringValue(7)
         //       The 2nd column is the traffic speed in that polygonal section
         val speed = instance.value(1)
         //       The 13th column is a well-known address inside New York
         val well_known_address = instance.stringValue(12)

         log_msg(DEBUG, "speed is: " + speed +
                   "\n         inside polygonal section " + polygon_str +
                   "\n         reference point: '" + well_known_address + "'")

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
            log_msg(WARNING, "ignoring polygon around reference address: '" +
                      well_known_address +
                      "'\n   not-parseable poly-points: " + polygon_str)

     }
     // Print those polygonal subsections of New York City that happen to have
     // now the slowest and the fastest speeds
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

           return list_valid_polyg
     }
     return null
}


/*
 *   MAIN PROGRAM
 *
 */


def main() {

     /* Take the time of the download of the Real-Time Speed URL, because
      * this time is used to correlate it with the samples of traffic volume
      * counts
      *
      *      NYC_Traffic_Volume_Count_URL (see definition above)
      *
      * because the latter has samples per hour of the day and per day (of
      * the week), ie., a sample for hour 12PM-1PM on Mondays, let's say,
      * according to the City of New York's Open Data.
      * (see definition of NYC_Traffic_Volume_Count_URL above)
      */

     val download_time = Calendar.getInstance().getTime()

     /* Convert the New York City Real-Time Speed URL to WEKA */

     val realt_speed_downldr =
                new convert_LinkSpeed_CSV_URL_to_WEKA_Serialized_insts(
                               download_time
                           )

     realt_speed_downldr.ETL_OpenData_CSV_URL_into_WEKA()

     val polygon_realt_speeds =
                 load_NewYork_traffic_speed_per_polygon_in_the_city(
                                   Config.Final_LinkSpeed_WEKA_SerInsts_fname
                                                                   )

     /* Process the Traffic Volume Counts per segment and per hour to
      * unite it with the Real-Time Traffic Speed.
      */

     val traffic_downldr =
             new convert_Traffic_Volume_Cnt_CSV_URL_to_WEKA_Serialized_insts(
                           download_time
                        )

     traffic_downldr.ETL_OpenData_CSV_URL_into_WEKA()

     /* Open the ESRI Shapefile with the New York City LION Single-Line Street
      * GeoDB. This ESRI Shapefile was a download from an ETL in project:
      *
      * https://github.com/je-nunez/Querying_NYC_Single_Line_Street_Base
      *
      */

     val nyc_lion = new ExistingShapefile(Config.NYC_LION_Polyline_Street_Map)
     nyc_lion.open()
     val lion_in_realt_speeds = new DefaultFeatureCollection()
     nyc_lion.filter_geom_features(polygon_realt_speeds, lion_in_realt_speeds)

    println("Number of filtered segments: " + lion_in_realt_speeds.size)
}


/*
 * Entry point
 */

main

