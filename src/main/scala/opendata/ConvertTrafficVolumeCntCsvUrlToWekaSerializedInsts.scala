package src.main.scala.opendata

import _root_.java.util.Date

import src.main.scala.logging.Logging._
import src.main.scala.config.Config


class ConvertTrafficVolumeCntCsvUrlToWekaSerializedInsts(
     val srcTrafficVolumeCntCsvUrl: String,
     val finalTrafficVolumeWekaSerializedFName: String,
     val currentTrafficVolumeDownloadTime: Date
  ) extends ConvertOpenDataCsvUrlToWekaSerializedInsts(
         srcTrafficVolumeCntCsvUrl,
         finalTrafficVolumeWekaSerializedFName,
         currentTrafficVolumeDownloadTime
  ) {


  override val cvsExpectedHeaderFields = Config.parserOpenDataCsv.
                                          csvHeadersNycTraffVolumeCountsUrl

  override val optionsConversionCsvToWeka = Config.parserOpenDataCsv.
                                       optionsWekaCsv2SerInstTraffVolumeCnt

  override val intermediateCleanCsvFName = Config.parserOpenDataCsv.
                                             intermCleanCsvFNameTraffVolume

  /*
   * constructor this(currentDownloadTime: Date)
   *
   * An auxiliary constructor which assumes that the NYC OpenData URL and
   * the destination WEKA Binary serialized instances will be to their
   * default location.
   *
   * @param currentDownloadTime the reference Datatime at which this
   *                                 processing is happening
   */

  def this(currentDownloadTime: Date) =
                   this(
                           Config.nycTrafficVolumeCount,
                           Config.finalWekaSerInstsFNameTraffVolume,
                           currentDownloadTime
                      )


  override def transformCsvHeaderLine(cvs_hdr_line: String):
                                                   Array[java.lang.String] =
         cvs_hdr_line.split(",")

  /*
   * function: transformCsvDataLine
   *
   * Check and tranform if the CSV data-line from the OpenData CSV URL has
   * all the fields that we expect and in the format we expect them, and
   * transform it: otherwise, ignore the bad input record.
   */

  protected def transformCsvDataLine(data_line: String,
                                        current_epoch: Long): String = {

    var line_values: Array[java.lang.String] = data_line.
                                          replaceAll("'", "-").
                                          split(",")

    // We expect that the parsing above returned 31 fields in this line

    if (line_values.length != 31) {
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

    for (field_index <- 0 to 30
             if (field_index <=1 || field_index >= 7)) {

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
  } // method transformCsvDataLine
}
