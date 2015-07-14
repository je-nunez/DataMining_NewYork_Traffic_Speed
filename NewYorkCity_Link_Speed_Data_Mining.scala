#!/usr/bin/env scala

import sys.process._
import scala.util.control.Exception._
import _root_.java.net.URL
import _root_.java.io.File
import _root_.java.text.SimpleDateFormat
import scala.collection.JavaConversions._
import _root_.java.net.URLEncoder

/* Polygon */

import org.geoscript.geometry.io._
import org.geoscript.geometry._
import org.geoscript.viewer._

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
 * New York City, and the classes below allow to find the intersection of
 * polygons, etc, these polygonal operations will allow us to find the
 * bottlenecks or optimum (faster) paths around different points.
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


val NYC_Traffic_Speed_URL = "http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt"


/*
 * function: error_msg
 *
 * Handle logging error messages
 */

val print_errors = 0

def error_msg(err_msg: String) {
    // Better to do with an error-level, like in Unix
    // (ie., Debug, Info, Notice, Warning, etc)

    if(print_errors != 0) System.err.println(err_msg)
}


/*
 * function: write_clean_CSV_header
 *
 * This is ETL. Check if the CVS header from the New York City Real-Time Link
 * Speed URL is the same as we expected, because if the CVS header has changed,
 * then the parser needs to change
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

      /*
       * println("DEBUG: received attributes: " + attributes.mkString(","))
       * println("DEBUG: expected: " + expected_attributes.mkString(","))
       */

      if( attributes.deep != expected_attributes.deep )
          // raise exception (and possibly abort, since the ETL parser needs
          // to be changed)
          throw new Exception("Format of New York City attributes has changed.")

      // The header line was verified: write it to the CSV file
      clean_csv_file.write(header_line + "\n")
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
          error_msg("WARNING: Ignoring line: " + data_line)
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
               error_msg("WARNING: Ignoring line: " + data_line)
               return
          }
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

def download_NYC_TrafficSpeed_to_clean_CSV(src_url: String, dest_file: String) {
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
                         error_msg("ERROR: I/O error occurred")
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
 * function: print_map
 *
 * Prints a map.
 * Currently it uses the Google Maps, but its purpose is to use the New York
 * City Open Data shapefiles, from GIS. (Like, e.g.,
 *
 *  https://data.cityofnewyork.us/Housing-Development/Building-Footprints/tb92-6tj8
 *
 * although this one has too much information, since it even has the building
 * foot-prints.
 */

def print_map(encoded_polyline: String, centroid: Point) {

      // URL-encode the encoded polygonal zone in "max_speed_polyline_enc"
 
      val url_pol: String = URLEncoder.encode(encoded_polyline, "UTF-8").
                                       replace("+", "%20")
 
      // Print the URL in Google Maps for it
      val centr_longitude = centroid.x
      val centr_latitude = centroid.y
      val gmap_url = "https://maps.googleapis.com/maps/api/staticmap?" +
                     f"center=$centr_longitude%6.6f,$centr_latitude%6.6f" +
                     "&zoom=14&size=800x800&maptype=roadmap" + 
                     "&path=fillcolor:0xFF000099%7Ccolor:0xFFFFFF00%7Cenc:" +
                     url_pol
      println(gmap_url)
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
 */

def load_NewYork_traffic_speed_per_polygon_in_the_city(weka_bsi_file: String) {
    // load the New York City Real-Time traffic speed loaded from
    //     http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt
    // into GeoScript/Java Topology Suite polygonal sections of New York City

    var sinst_in: SerializedInstancesLoader = new SerializedInstancesLoader();
    sinst_in.setSource(new File(weka_bsi_file));

    // Record the location which has the slowest speed
    var min_speed: Double = 9999999.0;
    var min_speed_polyline_centroid: Point = null;
    var min_speed_polyline_enc: String = "";
    var min_speed_relative_addr: String = "";
   
    // Record the location which has the fastest speed
    var max_speed: Double = -1.0;
    var max_speed_polyline_centroid: Point = null;
    var max_speed_polyline_enc: String = "";
    var max_speed_relative_addr: String = "";
   
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

         /*
         println("DEBUG: speed is: " + speed +
                 "\n           inside polygonal section " + polygon_str +
                 "\n           reference point: '" + well_known_address + "'")
         */

         val coords = catching(classOf[java.lang.RuntimeException]) opt polygon_str.split("\\s+").map {
                             case point_str: String => {
                                   val coord = point_str.split(",")
                                   (coord(0).toDouble, coord(1).toDouble)
                             }
                      }

         if (coords != null && coords.isDefined && coords.get.length > 0)  {
                      // It is a valid polygon
                      // println("DEBUG: polygon is: " + coords.get.mkString(" "))
                      // call GeoScript
                      val geoscript_line_string = builder.LineString(coords.get)
                      // println( geoscript_line_string )
                      // Find the centroid point of this polygon
                      val centroid = geoscript_line_string.centroid
                      // println("DEBUG: Centroid is " + centroid)
                      // See if this is the location with the less speed so far
                      if(speed < min_speed) {
                            min_speed = speed
                            min_speed_polyline_enc = polygon_enc
                            min_speed_polyline_centroid = centroid
                            min_speed_relative_addr = well_known_address
                      }
                      if(speed > max_speed) {
                            max_speed = speed
                            max_speed_polyline_enc = polygon_enc
                            max_speed_polyline_centroid = centroid
                            max_speed_relative_addr = well_known_address
                      }
         } else
            error_msg("WARNING: ignoring polygon around reference address: '" +
                      well_known_address +
                      "'\n   not-parseable poly-points: " + polygon_str)

     }
     // Print those polygonal subsections of New York City that happen to have
     // now the slowest and the fastest speeds
     if ( min_speed_polyline_enc != "" ) {
           println("Map of zone with the slowest speed at this moment: " + min_speed +
                   " (" + min_speed_relative_addr + ")")
           print_map(min_speed_polyline_enc, min_speed_polyline_centroid)
     }
     if ( max_speed_polyline_enc != "" ) {
           println("Map of zone with the fastest speed at this moment: " + max_speed +
                   " (" + max_speed_relative_addr + ")")
           print_map(max_speed_polyline_enc, max_speed_polyline_centroid)
     }
}


/*
 *   MAIN PROGRAM
 *
 */

// The temporary filename is "New_York_City_Link_Speed.csv" because WEKA will
// use the filename (without extension) as the @relation name in the ARFF file

val temp_csv_fname = "New_York_City_Link_Speed.csv"

// First pass of the parser: download URL and clean the records to a CSV file

download_NYC_TrafficSpeed_to_clean_CSV(NYC_Traffic_Speed_URL, temp_csv_fname)

// Second pass of the parser: convert CSV to BSI -SerializedInstances-, finding
// nominal attributes, etc.

val dest_bsi_fname = "New_York_City_Link_Speed.bsi"
convert_clean_CSV_to_WEKA_SerializedInstancesSaver(temp_csv_fname,
                                                   dest_bsi_fname)

new File(temp_csv_fname).delete()

load_NewYork_traffic_speed_per_polygon_in_the_city(dest_bsi_fname)

