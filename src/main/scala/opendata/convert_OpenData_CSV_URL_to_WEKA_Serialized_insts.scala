
package src.main.scala.opendata

import _root_.java.util.Date
import _root_.java.io.File

import weka.core.Instances
import weka.core.converters.CSVLoader
import weka.core.converters.SerializedInstancesSaver

import src.main.scala.logging.Logging._

/*
 * class: convert_OpenData_CSV_URL_to_WEKA_Serialized_insts
 *
 * An ETL base class (Extract-Transform-Load). Its purpose is to download a
 * (OpenData) CSV URL, to transform it, and to convert it into a WEKA
 * Serialized Instance file.
 */

abstract class convert_OpenData_CSV_URL_to_WEKA_Serialized_insts(
     val src_OpenData_CSV_URL: String,
     val dest_WEKA_Serialized_fname: String,
     val at_current_download_time: Date
   ) {


   // The expected fields to find at the header of the OpenData CSV

   val expected_header_fields: Array[String]

   // The options to set for the WEKA CSVLoader recognizing nominal,
   // date, and other attributes (fields in the CSV) which may be
   // difficult to recognize automatically

   val CSV_to_WEKA_options: Array[String]

   // the local filename to hold the intermediate "clean" CSV

   val intermediate_clean_CSV_fname: String


   protected def transform_CSV_header_line(cvs_hdr_line: String):
                                                   Array[java.lang.String]

   /*
    * function: write_clean_CSV_header
    *
    * Check if the CSV header from the OpenData CSV URL is the same as we
    * expected, because if the CSV header has changed, then the parser
    * needs to change
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
    * Check if the CSV data-line from the OpenData CSV URL has all the
    * fields that we expect and in the format we expect them, and
    * transform it: otherwise, ignore the bad input record, returning
    * null in such a case.
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
  } // write_clean_CSV_data_line


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
  } // method download_and_clean_into_intermediate_CSV


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
  } // method ETL_OpenData_CSV_URL_into_WEKA

}

