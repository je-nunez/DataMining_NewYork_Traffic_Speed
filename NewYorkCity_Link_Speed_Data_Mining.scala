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


/* Internal packages of this project. In this very first version, they
 * have this simple prefix, but will surely change. */

import src.main.scala.logging.Logging._
import src.main.scala.config.Config
import src.main.scala.graphics.shapefile._
import src.main.scala.types.Speed_in_PolygonalSection
import src.main.scala.utils.conversion.String_to_Geom
import src.main.scala.opendata._
import src.main.scala.model.Current_Real_Time_Traffic_Speeds



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

     val speed_model = new Current_Real_Time_Traffic_Speeds(
                                   Config.Final_LinkSpeed_WEKA_SerInsts_fname
                              )

     val polygon_realt_speeds =
                     speed_model.load_traffic_speed_per_polygon_in_the_city()

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

