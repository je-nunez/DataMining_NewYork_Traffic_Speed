#!/usr/bin/env scala -language:postfixOps

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


/* WEKA Machine Learning */

import weka.core.Instances
// import weka.core.converters.ArffSaver
import weka.core.converters.SerializedInstancesSaver
import weka.core.converters.CSVLoader
import weka.core.converters.SerializedInstancesLoader


// URL for the New York City Real-Time Link (Traffic) Speed (CSV format)

val NYC_Traffic_Speed_URL = "http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt"


/* This is the lastest Traffic Volume Counts per each segment of each street
 * and per-hour of the day in New York City. This URL doesn't have real-time
 * data, the latest sample of volume traffic was taken in 2013-2012.
 *
 * The master URL is:
 *
 * https://data.cityofnewyork.us/NYC-BigApps/Traffic-Volume-Counts-2012-2013-/p424-amsu
 *
 * where several formats are exported, like XML and JSON -which has a good
 * description of the fields in this dataset:
 *
 * https://data.cityofnewyork.us/api/views/p424-amsu/rows.json?accessType=DOWNLOAD
 *
 * but for current versions of WEKA, the CSV format is easier to read.
 */

val NYC_Traffic_Volume_Count_URL =
       "https://data.cityofnewyork.us/api/views/p424-amsu/rows.csv?accessType=DOWNLOAD"


/* The way to join both URLs, the Real-Time Link (Traffic) Speed -which uses
 * polygonal coordinates of sub-sections in the New York City- and the Traffic
 * Volume Counts -which uses segments IDs of each part of streets in NYC- is
 * necessary to employ the the Department of City Planning's LION Single Line
 * Street Base Map,
 *
 *     https://data.cityofnewyork.us/City-Government/LION/2v4z-66xt
 *
 *     http://www.nyc.gov/html/dcp/download/bytes/nyc_lion15b.zip
 *
 * which gives the polygonal coordinates of each segment ID, and then, with this
 * polygonal coordinates, to do a polygon intersection between both polygons
 * (Traffic Volume Counts' versus Real-Time Link (Traffic) Speed's) using the
 * GeoScript/GeoTools/JTS libraries.
 *
 * This is an example of the LION file as a master data:
 *
 *   http://www.nyc.gov/html/dot/html/motorist/truck_route_nyc_metadata.html
 *
 * Project:
 *
 *   https://github.com/je-nunez/Querying_NYC_Single_Line_Street_Base
 *
 * is an example of using the above libraries with NYC LION File GeoDataBase.
 * We use this other project for the download and the ETL of the LION GeoDB,
 * since it can be done only once (while the NYC Deparment of City Planning
 * doesn't update the LION GeoDB, which it is done only quarterly, according
 * to:
 *     Resource Maintenance -> Update Frequency
 *
 * in:
 *
 *     http://www.nyc.gov/html/dcp/pdf/bytes/lion_metadata.pdf?v=15b
 */

// Where the ETL left NYC LION import into an ESRI Shapefile:

val NYC_LION_Polyline_Street_Map = "etl_dest_shp/nyc_data_exploration.shp"


/* Let's say that this is our objective speed, so the New York City can be
 * crossed from extreme to extreme in 15 minutes, at this optimum
 * sustained speed. (Higher speeds than this are ideal and better, of course,
 * but more costly.)
 *
 * This target of the optimum speed can be instead a variable determined at
 * run-time (below the maximum of the real-time speeds of traffic is indeed
 * calculated by the code, but this dinamyc maximum can be either slower
 * than this target optimum, or an outlier value that is too high from the
 * target optimum).
 */

val target_optimum_NYC_speed: Double = 80.0

/*
 * function: log_msg
 *
 * Handle logging error messages
 *
 * Better to do with an error-level, like in Unix (ie., Debug, Info, Notice,
 * Warning, etc), and this is why we don't use a Boolean value here, but an
 * integer representing the logging-level threshold.
 */

object LoggingLevel extends Enumeration {
     type LoggingLevel = Value

     val EMERGENCY = Value(0)
     val ALERT = Value(1)
     val CRITICAL = Value(2)
     val ERROR = Value(3)
     val WARNING = Value(4)
     val NOTICE = Value(5)
     val INFO = Value(6)
     val DEBUG = Value(7)
}

import LoggingLevel._

val loggingThreshold = NOTICE

def log_msg(level: LoggingLevel, err_msg: String) 
{
    // if(level.toInt <= loggingThreshold.toInt) {

    if(level <= loggingThreshold) {
        System.err.println(level.toString + ": " + err_msg)
    }
}


/*
 * function: write_clean_CSV_header
 *
 * This is ETL. Check if the CVS header from the New York City Real-Time Link
 * Speed URL is the same as we expected, because if the CVS header has changed,
 * then the parser needs to change
 *
 * @param header_line the CVS header line from the Real-Time speed URL to check
 * @param clean_csv_file the clean (ETL-ed filtered) CVS file to write to
 */

def write_clean_CSV_header(header_line: String,
                                clean_csv_file: java.io.FileWriter) {

      var attributes: Array[java.lang.String] = header_line.
                                                  replaceAllLiterally("\"", "").
                                                  split("\t")

      /* Check if the attributes in the NYC header line are our
       * expected attributes, so this parser is quite strict.
       */

      val expected_attributes= Array(
                                      "Id",
                                      "Speed",
                                      "TravelTime",
                                      "Status",
                                      "DataAsOf",
                                      "linkId",
                                      "linkPoints",
                                      "EncodedPolyLine",
                                      "EncodedPolyLineLvls",
                                      "Owner",
                                      "Transcom_id",
                                      "Borough",
                                      "linkName"
                                     )

      log_msg(DEBUG, "received attributes: " + attributes.mkString(","))
      log_msg(DEBUG, "expected: " + expected_attributes.mkString(","))

      if( attributes.deep != expected_attributes.deep ) {
          // raise exception (and possibly abort this program, since its ETL
          // parser needs to be changed)
          log_msg(EMERGENCY, "Format of Real-Time Speed attributes has " +
                               " changed: "  +
                               "\n   expected: " +
                                       expected_attributes.mkString(",") +
                               "\n   downloaded: " +
                                       attributes.mkString(",")
                    )
          throw new Exception("Format of Real-Time Speed attributes has " +
                               "changed.")
      }

      // The header line was verified: write it to the CSV file
      clean_csv_file.write(header_line + "\n")
}


/*
 * function: convert_string_to_multiline_geom
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

def convert_string_to_multiline_geom(in_s: String):
            jts.geom.Geometry =
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
          log_msg(DEBUG, "Geometry instances is " + geometry_instance)
          return geometry_instance
      } else
          return null
}


/*
 * function: write_clean_CSV_data_line
 *
 * This is ETL. Check if the CVS data-line from the New York City Real-Time
 * Link Speed URL has all the fields that we expect and in the format we
 * expect them: otherwise, ignore the bad input record
 */

def write_clean_CSV_data_line(data_line: String,
                              clean_csv_file: java.io.FileWriter) {

      // a better parser is needed of the lines that the
      // New York City Link Speed Query gives us
      var line_values: Array[java.lang.String] = data_line.
                                                 replaceAll("\\\\", "\\\\\\\\").
                                                 stripPrefix("\"").
                                                 stripSuffix("\"").
                                                 split("\"\\s+\"")

      // We expect that the parsing above returned 13 fields in this line

      if(line_values.length != 13) {
          log_msg(WARNING, "Ignoring: doesn't have right number of fields: "
                             + data_line)
          return
      }

      // Validate that the fifth field of the New York City Traffic Speed is
      // a date in the format "M/d/yyyy HH:mm:ss"
      val date_fmt = "M/d/yyyy HH:mm:ss"
      // JODA is better
      val nyc_date_time_fmt = new java.text.SimpleDateFormat(date_fmt)
      nyc_date_time_fmt.setLenient(false)

      try {
          val d = nyc_date_time_fmt.parse(line_values(4))
      } catch {
          case e: java.text.ParseException => {
               log_msg(WARNING, "Ignoring: wrong date-time in 5th field: " +
                                  data_line)
               return
          }
      }

      // the seventh field is the list of coordinates to which this speed
      // applies
      val geometry_inst = convert_string_to_multiline_geom(line_values(6))
      if(geometry_inst == null) {
          // This field didn't seem to have a valid, comma-separated, list
          // of coordinates: then ignore this line in the CSV file
          log_msg(WARNING, "Ignoring: wrong geometry in 7th field: " +
                             data_line)
          return
      }

      // Write this data line to the CSV file
      clean_csv_file.write(data_line + "\n")
}


/*
 * function: download_NYC_TrafficSpeed_to_clean_CSV
 *
 * This is ETL. Download the CVS from the New York City Real-Time URL and
 * validates its header and filters the records that are valid.
 */

def download_NYC_TrafficSpeed_to_clean_CSV(src_url: String,
                                           dest_file: String) {
      try {
            val src = scala.io.Source.fromURL(src_url)
            val out = new java.io.FileWriter(dest_file)

            var line_number = 1

            for (line <- src.getLines) {
                // out.write(line + "\n")
                if ( line_number == 1 )
                     write_clean_CSV_header(line, out)
                else
                     write_clean_CSV_data_line(line, out)
                line_number += 1
            }

            src.close()
            out.close
      } catch {
            case e: java.io.IOException => {
                         log_msg(ERROR, "I/O error occurred" + e.getMessage)
                       }
      }
}


/*
 * Not in use: see convert_clean_CSV_to_WEKA_SerializedInstancesSaver()
 *
def convert_clean_CSV_to_WEKA_ARFF(src_csv: String, dest_arff: String) {

      // Modified from https://weka.wikispaces.com/Converting+CSV+to+ARFF
      // to add the format of our dates, and nominal attributes

      var cvs_in: CSVLoader = new CSVLoader();
      cvs_in.setOptions(Array(
                              "-D", "5",
                              "-format", "M/d/yyyy HH:mm:ss",
                              "-S", "7-9"
                             )
                       );
      cvs_in.setSource(new File(src_csv));
      var instances_in: Instances = cvs_in.getDataSet();

      var arff_out: ArffSaver = new ArffSaver();
      arff_out.setInstances(instances_in);
      arff_out.setFile(new File(dest_arff));
      arff_out.writeBatch();
}
 *
 */


/*
 * function: convert_clean_CSV_to_WEKA_SerializedInstancesSaver
 *
 * Converts the CSV file to a binary file in WEKA SerializedInstancesSaver
 * format (".bsi" extension) -that currently is the same as the Java
 * serialized object format.
 */

def convert_clean_CSV_to_WEKA_SerializedInstancesSaver(src_csv: String,
                                                       dest_bsi: String) {

      var cvs_in: CSVLoader = new CSVLoader();
      /* Some options for the New York City's Traffic Speed CVS, ie., which
       * attributes we know are date (and which date format), and which
       * attributes we know are string (and not to be analyzed as nominal
       * values)
       */
      cvs_in.setOptions(Array(
                              "-D", "5",
                              "-format", "M/d/yyyy HH:mm:ss",
                              "-S", "7-9"
                             )
                       );
      cvs_in.setSource(new File(src_csv));
      var instances_in: Instances = cvs_in.getDataSet();

      var sinst_out: SerializedInstancesSaver = new SerializedInstancesSaver();
      sinst_out.setInstances(instances_in);
      sinst_out.setFile(new File(dest_bsi));
      sinst_out.writeBatch();
}


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


// This is a simplification of the WEKA instances

case class Speed_in_PolygonalSection(speed: Double, 
                                     geometry: jts.geom.Geometry,
                                     polygon_encoded: String,
                                     centroid: Point, well_known_addr: String);


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
 * foot-prints.
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
           if (polygon_zone.speed >= target_optimum_NYC_speed)
                 color = 0x00FF00
           else {
                 val delta_speed = ( polygon_zone.speed - min_speed ) /
                                      ( target_optimum_NYC_speed - min_speed )

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

         val geometry_instance = convert_string_to_multiline_geom(polygon_str)

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


def download_NYC_TrafficVolumeCount_to_clean_CSV(src_url: String,
                                                 needed_time: Date,
                                                 dest_file: String)
{
     // TODO: polymorphism with appropiate sub-classes, or filtering functions
     //       as additional parameters
     try {
           val src = scala.io.Source.fromURL(src_url)
           val out = new java.io.FileWriter(dest_file)

           var line_number = 1

           for (line <- src.getLines) {
               // out.write(line + "\n")
               if ( line_number == 1 ) {
                    // TODO
                    // write_clean_CSV_header_TrafVolCnt(line, out)
               } else {
                    /* We need to filter only the Traffic Volume Counts
                     * for the hour and day of the week in 'needed_time' 
                     * and ignore all the counts for the other hours/days
                     */
                    // TODO
                    // write_clean_CSV_data_line_TrafVolCnt(line,
                    //                                      needed_time, out)
               }
               line_number += 1
          }

          src.close()
          out.close()
     } catch {
           case e: java.io.IOException => {
                        log_msg(ERROR, "I/O error occurred" + e.getMessage)
                      }
     }

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

     /* The temporary filename is "New_York_City_Link_Speed.csv" because WEKA
      * will use the filename (without extension) as the @relation name in
      * the ARFF file
      */

     val tmp_RT_Speed_CsvFname = "New_York_City_Link_Speed.csv"

     /* First pass of the parser: download URL and clean the records to a
      * CSV file */

     download_NYC_TrafficSpeed_to_clean_CSV(NYC_Traffic_Speed_URL,
                                            tmp_RT_Speed_CsvFname)

     /* Second pass of the parser: convert CSV to BSI -SerializedInstances-,
      * finding nominal attributes, etc. */

     val dest_RT_Speed_Bsi_Fname = "New_York_City_Link_Speed.bsi"
     convert_clean_CSV_to_WEKA_SerializedInstancesSaver(tmp_RT_Speed_CsvFname,
                                                      dest_RT_Speed_Bsi_Fname)

     new File(tmp_RT_Speed_CsvFname).delete()

     val polygon_realt_speeds =
            load_NewYork_traffic_speed_per_polygon_in_the_city(
                                             dest_RT_Speed_Bsi_Fname
                                                              )

     /* Process the Traffic Volume Counts per segment and per hour to
      * unite it with the Real-Time Traffic Speed.
      * We need to correlate the Traffic Volume Counts only with the
      * date and time of the download of the Real-Time Speed, and
      * ignore all other Traffic Volume Counts for other hours/days.
      */

     val tmp_Volum_Count_CsvFname = "New_York_City_Volum_Cnt.csv"

     download_NYC_TrafficVolumeCount_to_clean_CSV(NYC_Traffic_Volume_Count_URL,
                                                  download_time, 
                                                  tmp_Volum_Count_CsvFname)
}


/*
 * Entry point
 */

main

