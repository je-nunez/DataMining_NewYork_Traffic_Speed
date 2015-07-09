#!/usr/bin/env scala

import sys.process._
import scala.util.control.Exception._
import _root_.java.net.URL
import _root_.java.io.File
import _root_.java.text.SimpleDateFormat
import scala.collection.JavaConversions._

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
          System.err.println("WARNING: Ignoring line: " + data_line)
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
               System.err.println("WARNING: Ignoring line: " + data_line)
               return
          }
      }

      // Write this data line to the CSV file
      clean_csv_file.write(data_line + "\n")
}

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
                         System.err.println("ERROR: I/O error occurred")
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


def load_NewYork_traffic_speed_per_polygon_in_the_city(weka_bsi_file: String) {
    // load the New York City Real-Time traffic speed loaded from
    //     http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt
    // into GeoScript/Java Topology Suite polygonal sections of New York City

    var sinst_in: SerializedInstancesLoader = new SerializedInstancesLoader();
    sinst_in.setSource(new File(weka_bsi_file));

    for (instance <- Iterator.continually(sinst_in.getNextInstance(null)).
                                 takeWhile(_ != null)) {

         // Split the fields of this WEKA instance:
         //       The 7th column is the polygonal section inside New York City
         val polygon_str = instance.stringValue(6)
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
                      // println("DEBUG: polygon is: " + coords.get.mkString(" "))
                      // call GeoScript
                      // builder.LineString(Array((0.0,0.0), (1.0,1.0), (2.0,4.0), (0.0,0.0)))
                      val geoscript_line_string = builder.LineString(coords.get)
                      println( geoscript_line_string )
                      // res1: org.geoscript.geometry.LineString = LINESTRING (...)
         } else
            System.err.println("WARNING: ignoring polygon around " +
                               "reference address: '" + well_known_address +
                               "'\n   not-parseable poly-points: " + polygon_str)

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

