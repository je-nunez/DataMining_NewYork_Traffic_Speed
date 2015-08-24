#!/usr/bin/env scala -deprecation

import _root_.java.util.Calendar
import scala.collection.JavaConversions._

/* Use GeoTools's DefaultFeatureCollection for the intersection
 * between New York City's Department of City Planning LION
 * Single-line Street Segments Geographical DB and the NYC
 * Real-Time Traffic Speed per polygonal zone in the New York
 */

import org.geotools.feature.DefaultFeatureCollection

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
 *      relying all the functionality to the application packages above
 *
 */

object mainNewYorkCityLinkSpeedDataMining {

  def main(args: Array[String]) {

    // Take the time of the download of the Real-Time Speed URL, because
    // this time is used to correlate it with the samples of traffic volume
    // counts
    //
    //      NYC_Traffic_Volume_Count_URL (see definition above)
    //
    // because the latter has samples per hour of the day and per day (of
    // the week), ie., a sample for hour 12PM-1PM on Mondays, let's say,
    // according to the City of New York's Open Data.
    // (see definition of NYC_Traffic_Volume_Count_URL above)

    val download_time = Calendar.getInstance().getTime()

    // Convert the New York City Real-Time Speed URL to WEKA

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

    // Process the Traffic Volume Counts per segment and per hour to
    // unite it with the Real-Time Traffic Speed.

    val traffic_downldr =
      new convert_Traffic_Volume_Cnt_CSV_URL_to_WEKA_Serialized_insts(
                         download_time
                     )

    traffic_downldr.ETL_OpenData_CSV_URL_into_WEKA()

    // Open the ESRI Shapefile with the New York City LION Single-Line Street
    // GeoDB. This ESRI Shapefile was a download from an ETL in project:
    //
    // https://github.com/je-nunez/Querying_NYC_Single_Line_Street_Base

    val nyc_lion = new ExistingShapefile(Config.NYC_LION_Polyline_Street_Map)
    nyc_lion.open()
    val lion_in_realt_speeds = new DefaultFeatureCollection()
    nyc_lion.filter_geom_features(polygon_realt_speeds, lion_in_realt_speeds)

    println("Number of filtered segments: " + lion_in_realt_speeds.size)
  }  // end of method main(...)
} // end of object mainNewYorkCityLinkSpeedDataMining

