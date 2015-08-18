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


/* WEKA Machine Learning */

import weka.core.Instances
// import weka.core.converters.ArffSaver
import weka.core.converters.SerializedInstancesSaver
import weka.core.converters.CSVLoader
import weka.core.converters.SerializedInstancesLoader


/* Internal packages of this project. In this very first version, they
 * have this simple prefix, but will surely change. */

import src.main.scala.logging.Logging._

import src.main.scala.config.Config

import src.main.scala.graphics.shapefile._

import src.main.scala.types.Speed_in_PolygonalSection

import src.main.scala.utils.conversion.String_to_Geom

/*
 * class: convert_OpenData_CSV_URL_to_WEKA_Serialized_insts
 *
 * An ETL base class (Extract-Transform-Load). Its purpose is to download a
 * (OpenData) CSV URL, to transform it, and to convert it into a WEKA
 * Serialized Instance class.
 */

abstract class convert_OpenData_CSV_URL_to_WEKA_Serialized_insts(
     val src_OpenData_CSV_URL: String,
     val dest_WEKA_Serialized_fname: String,
     val at_current_download_time: Date
   ) {


   val expected_header_fields: Array[String]
   val CSV_to_WEKA_options: Array[String]
   val intermediate_clean_CSV_fname: String


   protected def transform_CSV_header_line(cvs_hdr_line: String):
                                                   Array[java.lang.String]

   /*
    * function: write_clean_CSV_header
    *
    * This is ETL. Check if the CSV header from the OpenData CSV URL is the
    * same as we expected, because if the CSV header has changed, then the
    * parser needs to change
    *
    * @param header_line the CSV header line from the CSV URL to check
    * @param clean_csv_file the clean (ETL-ed filtered) CSV file to write to
    */

   protected def write_clean_CSV_header(header_line: String,
                                   clean_csv_file: java.io.FileWriter) {
   
         var attributes: Array[java.lang.String] = 
                                 transform_CSV_header_line(header_line)

         /* Check if the attributes in the NYC header line are our
          * expected attributes, so this parser is quite strict.
          */

         log_msg(DEBUG, "received attributes: " + attributes.mkString(","))
         log_msg(DEBUG, "expected: " + expected_header_fields.mkString(","))
   
         if( attributes.deep != expected_header_fields.deep ) {
             // raise exception (and possibly abort this program, since its
             // ETL parser needs to be changed)
             log_msg(EMERGENCY, "Format of OpenData CSV header attributes has"
                                  + " changed: "  +
                                  "\n   expected: " +
                                       expected_header_fields.mkString(",") +
                                  "\n   downloaded: " +
                                       attributes.mkString(",")
                          )
             throw new Exception("Format of the OpenData CSV header" +
                                 " attributes has changed.")
         }

         // The header line was verified: write it to the CSV file
         clean_csv_file.write(header_line + "\n")
   }


   /*
    * function: transform_CSV_data_line
    *
    * Check and tranform if the CSV data-line from the OpenData CSV URL has
    * all the fields that we expect and in the format we expect them, and
    * transform it: otherwise, ignore the bad input record.
    */

  protected def transform_CSV_data_line(data_line: String,
                                        current_epoch: Long): String


   /*
    * function: write_clean_CSV_data_line
    *
    * Check if the CSV data-line from the OpenData URL has all the fields
    * that we expect and in the format we expect them: otherwise, ignore the
    * bad input record
    */

  protected def write_clean_CSV_data_line(data_line: String,
                                          current_epoch: Long,
                                          clean_csv_file: java.io.FileWriter)
  {

         val transformed_line = transform_CSV_data_line(data_line,
                                                        current_epoch)

         if(transformed_line != null)
             // Write this data line to the CSV file
             clean_csv_file.write(transformed_line + "\n")
  }



  /*
   * function: convert_intermediate_CSV_to_WEKA_SerInsts
   *
   * Converts the CSV file to a binary file in WEKA SerializedInstancesSaver
   * format (".bsi" extension) -that currently is the same as the Java
   * serialized object format.
   */

  protected def convert_intermediate_CSV_to_WEKA_SerInsts() {

          var cvs_in: CSVLoader = new CSVLoader();
          /* Some options for the cleaned OpenData CSV, ie., which attributes
           * we know are date (and which date format), and which attributes
           * we know are string (and not to be analyzed as nominal values)
           */
          cvs_in.setOptions(CSV_to_WEKA_options)
          cvs_in.setSource(new File(intermediate_clean_CSV_fname));
          var instances_in: Instances = cvs_in.getDataSet();

          var sinst_out: SerializedInstancesSaver =
                                             new SerializedInstancesSaver();
          sinst_out.setInstances(instances_in);
          sinst_out.setFile(new File(dest_WEKA_Serialized_fname));
          sinst_out.writeBatch();
  }



  protected def download_and_clean_into_intermediate_CSV() {

          val current_epoch = at_current_download_time.getTime()

          try {
                val src = scala.io.Source.fromURL(src_OpenData_CSV_URL)
                val out = new java.io.FileWriter(intermediate_clean_CSV_fname)

                var line_number = 1

                for (line <- src.getLines) {
                    // out.write(line + "\n")
                    if ( line_number == 1 )
                         write_clean_CSV_header(line, out)
                    else
                         write_clean_CSV_data_line(line, current_epoch, out)
                    line_number += 1
                }

                src.close
                out.close
          } catch {
                case e: java.io.IOException => {
                           log_msg(ERROR, "I/O error occurred" + e.getMessage)
                        }
          }
  }


  def ETL_OpenData_CSV_URL_into_WEKA() {

          /* download the OpenData CSV URL and clean it into CSV filename 
           * "intermediate_clean_CSV_fname"
           */
          download_and_clean_into_intermediate_CSV()

          /* Second pass of the parser: convert CSV to BSI
           * -SerializedInstances-, finding nominal attributes, etc. */

          convert_intermediate_CSV_to_WEKA_SerInsts()

          /* delete the intermediate, clean CSV file */
          new File(intermediate_clean_CSV_fname).delete()
  }


}


class convert_LinkSpeed_CSV_URL_to_WEKA_Serialized_insts(
     val src_LinkSpeed_CSV_URL: String,
     val dest_Speed_WEKA_Serialized_fname: String,
     val at_Speed_current_download_time: Date
   ) extends convert_OpenData_CSV_URL_to_WEKA_Serialized_insts(
         src_LinkSpeed_CSV_URL,
         dest_Speed_WEKA_Serialized_fname,
         at_Speed_current_download_time
   ) {


   override val expected_header_fields = Config.OpenData_CSV_Parser.
                                              NYC_LinkSpeed_Fields_Header

   override val CSV_to_WEKA_options = Config.OpenData_CSV_Parser.
                                            NYC_LinkSpeed_CSV_to_WEKA_SerInst

   override val intermediate_clean_CSV_fname = Config.OpenData_CSV_Parser.
                                            Intermediate_LinkSpeed_clean_CSV

   override def transform_CSV_header_line(cvs_hdr_line: String):
                                                   Array[java.lang.String]
       =
         cvs_hdr_line.replaceAllLiterally("\"", "").split("\t")

   /*
    * constructor this(at_Speed_current_download_time: Date)
    *
    * An auxiliary constructor which assumes that the NYC OpenData URL and
    * the destination WEKA Binary serialized instances will be to their
    * default location.
    *
    * @param at_Speed_current_download_time the reference Datatime at which
    *                                       this processing is happening
    */

   def this(at_Speed_current_download_time: Date) =
                        this(
                               Config.NYC_Traffic_Speed_URL,
                               Config.Final_LinkSpeed_WEKA_SerInsts_fname,
                               at_Speed_current_download_time
                           )


   /*
    * function: transform_CSV_data_line
    *
    * Check and tranform if the CSV data-line from the OpenData CSV URL has
    * all the fields that we expect and in the format we expect them, and
    * transform it: otherwise, ignore the bad input record.
    */

  protected def transform_CSV_data_line(data_line: String,
                                        current_epoch: Long): String = {

         /* How old (in milliseconds) can be the real-time sample we have
          * received in data_line: the oldest real-time sample we tolerate
          * is 30 minutes old (me multiply by * 1000 because Java uses
          * millisecs)
          */

         val max_age_tolerance_in_time = (30 * 60 * 1000)

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
             return null
         }

         // Validate that the fifth field of the New York City Traffic Speed
         // is a date in the format "M/d/yyyy HH:mm:ss"
         val date_fmt = "M/d/yyyy HH:mm:ss"
         // JODA is better
         val nyc_date_time_fmt = new java.text.SimpleDateFormat(date_fmt)
         nyc_date_time_fmt.setLenient(false)

         var d: Date = null
         try {
             d = nyc_date_time_fmt.parse(line_values(4))
         } catch {
             case e: java.text.ParseException => {
                  log_msg(WARNING, "Ignoring: wrong date-time in 5th field: "
                                   + data_line)
                  return null
             }
         }

         val epoch_of_measure_in_line: Long = d.getTime()

         val diff_in_epochs: Long = (current_epoch - epoch_of_measure_in_line)

         if(diff_in_epochs > max_age_tolerance_in_time) {
             log_msg(WARNING, "Ignoring: date-time in 5th field is too old: "
                              + data_line)
             return null
         } else if(diff_in_epochs < 0) {
             // The sample in this line has a time ahead of us: it can be
             // trickier for us to correlate it with the other data, and this
             // is why can set the log_msg() to ERROR instead of WARNING, so
             // it is distinctive

             val abs_val_diff_in_epochs = -diff_in_epochs
             if(abs_val_diff_in_epochs > max_age_tolerance_in_time) {
                  log_msg(ERROR, "Ignoring: date-time in 5th field is ahead " +
                                 "of us by too much: " + abs_val_diff_in_epochs +
                                 " millisecs: current epoch: " + current_epoch +
                                 " epoch of sample: " + epoch_of_measure_in_line +
                                 " sample line: " + data_line)
                  return null
	     } else {
                  // just give a notice about the time of this sample but
                  // don't return from this function
                  log_msg(NOTICE, "a sample has date-time in 5th field ahead "
                                  + "of us by " + abs_val_diff_in_epochs +
                                  " millisecs: " + data_line)
             }
         }

         // the seventh field is the list of coordinates to which this speed
         // applies
         val geometry_inst = String_to_Geom(line_values(6))
         if(geometry_inst == null) {
             // This field didn't seem to have a valid, comma-separated, list
             // of coordinates: then ignore this line in the CSV file
             log_msg(WARNING, "Ignoring: wrong geometry in 7th field: " +
                              data_line)
             return null
         }

         return data_line
     }

}


class convert_Traffic_Volume_Cnt_CSV_URL_to_WEKA_Serialized_insts(
     val src_LinkSpeed_CSV_URL: String,
     val dest_Speed_WEKA_Serialized_fname: String,
     val at_Traffic_Vol_Cnt_current_download_time: Date
   ) extends convert_OpenData_CSV_URL_to_WEKA_Serialized_insts(
         src_LinkSpeed_CSV_URL,
         dest_Speed_WEKA_Serialized_fname,
         at_Traffic_Vol_Cnt_current_download_time
   ) {


   override val expected_header_fields = Config.OpenData_CSV_Parser.
                                          NYC_TrafficVolumeCnts_Fields_Header

   override val CSV_to_WEKA_options = Config.OpenData_CSV_Parser.
                                    NYC_TrafficVolumeCnt_CSV_to_WEKA_SerInst

   override val intermediate_clean_CSV_fname = Config.OpenData_CSV_Parser.
                                     Intermediate_TrafficVolumeCnt_clean_CSV

   /*
    * constructor this(at_current_download_time: Date)
    *
    * An auxiliary constructor which assumes that the NYC OpenData URL and
    * the destination WEKA Binary serialized instances will be to their
    * default location.
    *
    * @param at_current_download_time the reference Datatime at which this
    *                                 processing is happening
    */

   def this(at_current_download_time: Date) =
                   this(
                           Config.NYC_Traffic_Volume_Count_URL,
                           Config.Final_TrafficVolumeCnt_WEKA_SerInsts_fname,
                           at_current_download_time
                      )


   override def transform_CSV_header_line(cvs_hdr_line: String):
                                                   Array[java.lang.String]
       =
         cvs_hdr_line.split(",")

   /*
    * function: transform_CSV_data_line
    *
    * Check and tranform if the CSV data-line from the OpenData CSV URL has
    * all the fields that we expect and in the format we expect them, and
    * transform it: otherwise, ignore the bad input record.
    */

  protected def transform_CSV_data_line(data_line: String,
                                        current_epoch: Long): String = {

         var line_values: Array[java.lang.String] = data_line.
                                               replaceAll("'", "-").
                                               split(",")

         // We expect that the parsing above returned 31 fields in this line

         if(line_values.length != 31) {
             log_msg(WARNING, "Ignoring: doesn't have right number of fields: "
                              + data_line)
             return null
         }

         // Validate that the seventh field of New York City Traffic Volume
         // Count is a date in the format "M/d/yyyy"
         val date_fmt = "M/d/yyyy"
         // JODA is better
         val nyc_date_time_fmt = new java.text.SimpleDateFormat(date_fmt)
         nyc_date_time_fmt.setLenient(false)

         var d: Date = null
         try {
             d = nyc_date_time_fmt.parse(line_values(6))
         } catch {
             case e: java.text.ParseException => {
                  log_msg(WARNING, "Ignoring: wrong date in 7th field: " +
                                   data_line)
                  return null
             }
         }

         // Check that the fields 1,2, and 8 to 31 are non-negative integers

         for( field_index <- 0 to 30
                  if ( field_index <=1 || field_index >= 7 ) )
         {
              val field_value = line_values(field_index)
              var int_field_value: Int = -1
              try { 
                     int_field_value = field_value.toInt
              } catch { 
                     case e: NumberFormatException => {
                            log_msg(WARNING, "Ignoring: not an integer in " +
                                     (field_index + 1) + " field: value: " +
                                     field_value + "; line: " + data_line)
                            return null
                       }
              }

              if (int_field_value < 0) {
                  log_msg(WARNING, "Ignoring: not a non-negative int in " +
                                   (field_index + 1) + " field: value: " +
                                   int_field_value + "; line: " +
                                   data_line)
                  return null
              }
         }

         return line_values.mkString(",")
     }

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

}


/*
 * Entry point
 */

main

