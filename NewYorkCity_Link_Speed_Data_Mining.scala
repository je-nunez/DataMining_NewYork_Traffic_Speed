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
import src.main.scala.types.SpeedInPolygonalSection
import src.main.scala.utils.conversion.StringToGeom
import src.main.scala.opendata._
import src.main.scala.model.CurrentRealTimeTrafficSpeeds


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
    //      nycTrafficVolumeCount (see definition above)
    //
    // because the latter has samples per hour of the day and per day (of
    // the week), ie., a sample for hour 12PM-1PM on Mondays, let's say,
    // according to the City of New York's Open Data.
    // (see definition of nycTrafficVolumeCount above)

    val download_time = Calendar.getInstance().getTime()

    // Convert the New York City Real-Time Speed URL to WEKA

    val realt_speed_downldr =
      new ConvertTrafficSpeedCsvUrlToWekaSerializedInsts(
                               download_time
        )

    realt_speed_downldr.etlOpenDataCsvUrlIntoWeka()

    val speed_model = new CurrentRealTimeTrafficSpeeds(
                           Config.finalWekaSerInstsFNameTrafficSpeeds
                         )

    val polygon_realt_speeds =
      speed_model.loadTrafficSpeedPerPolygonInTheCity()

    // Process the Traffic Volume Counts per segment and per hour to
    // unite it with the Real-Time Traffic Speed.

    val traffic_downldr =
      new ConvertTrafficVolumeCntCsvUrlToWekaSerializedInsts(
                         download_time
                     )

    traffic_downldr.etlOpenDataCsvUrlIntoWeka()

    // Open the ESRI Shapefile with the New York City LION Single-Line Street
    // GeoDB. This ESRI Shapefile was a download from an ETL in project:
    //
    // https://github.com/je-nunez/Querying_NYC_Single_Line_Street_Base

    val nyc_lion = new ExistingShapefile(Config.nycLionMultiLineStreetMapFName)
    nyc_lion.open()
    val lion_in_realt_speeds = new DefaultFeatureCollection()
    nyc_lion.filter_geom_features(polygon_realt_speeds, lion_in_realt_speeds)

    println("Number of filtered segments: " + lion_in_realt_speeds.size)
  }  // end of method main(...)
} // end of object mainNewYorkCityLinkSpeedDataMining

