package src.main.scala.opendata

import _root_.java.util.Date

import src.main.scala.logging.Logging._
import src.main.scala.config.Config


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
     } // method transform_CSV_data_line
}

