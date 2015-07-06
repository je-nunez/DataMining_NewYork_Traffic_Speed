#!/usr/bin/env scala

import sys.process._
import _root_.java.net.URL
import _root_.java.io.File
import _root_.java.text.SimpleDateFormat

import weka.core.Instances
// import weka.core.converters.ArffSaver
import weka.core.converters.SerializedInstancesSaver
import weka.core.converters.CSVLoader



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

