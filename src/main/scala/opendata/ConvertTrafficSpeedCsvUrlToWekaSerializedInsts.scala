package src.main.scala.opendata

import _root_.java.util.Date

import src.main.scala.logging.Logging._
import src.main.scala.config.Config
import src.main.scala.utils.conversion.StringToGeom



class ConvertTrafficSpeedCsvUrlToWekaSerializedInsts(
     val srcTrafficSpeedCsvUrl: String,
     val finalSpeedWekaSerializedFName: String,
     val currentSpeedDownloadTime: Date
  ) extends ConvertOpenDataCsvUrlToWekaSerializedInsts(
         srcTrafficSpeedCsvUrl,
         finalSpeedWekaSerializedFName,
         currentSpeedDownloadTime
  ) {


  override val cvsExpectedHeaderFields = Config.parserOpenDataCsv.
                                              csvHeadersNycLinkSpeedUrl

  override val optionsConversionCsvToWeka = Config.parserOpenDataCsv.
                                            optionsWekaCsv2SerInstLinkSpeed

  override val intermediateCleanCsvFName = Config.parserOpenDataCsv.
                                            intermCleanCsvFNameTrafficSpeeds

  override def transformCsvHeaderLine(cvs_hdr_line: String):
                                                   Array[java.lang.String] =
    cvs_hdr_line.replaceAllLiterally("\"", "").split("\t")

  /*
   * constructor this(currentSpeedDownloadTime: Date)
   *
   * An auxiliary constructor which assumes that the NYC OpenData URL and
   * the destination WEKA Binary serialized instances will be to their
   * default location.
   *
   * @param currentSpeedDownloadTime the reference Datatime at which
   *                                       this processing is happening
   */

  def this(currentSpeedDownloadTime: Date) =
                        this(
                               Config.nycTrafficSpeedUrl,
                               Config.finalWekaSerInstsFNameTrafficSpeeds,
                               currentSpeedDownloadTime
                           )


  /*
   * function: transformCsvDataLine
   *
   * Check and tranform if the CSV data-line from the OpenData CSV URL has
   * all the fields that we expect and in the format we expect them, and
   * transform it: otherwise, ignore the bad input record.
   */

  protected def transformCsvDataLine(data_line: String,
                                        current_epoch: Long): String = {

    /* How old (in milliseconds) can be the real-time sample we have received
     * in data_line: the oldest real-time sample we tolerate is 30 minutes
     * old (me multiply by * 1000 because Java uses millisecs)
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

    if (line_values.length != 13) {
      log_msg(WARNING, "Ignoring: doesn't have right number of fields: " +
                       data_line)
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

    if (diff_in_epochs > max_age_tolerance_in_time) {
        log_msg(WARNING, "Ignoring: date-time in 5th field is too old: "
                         + data_line)
        return null
    } else if (diff_in_epochs < 0) {
        // The sample in this line has a time ahead of us: it can be trickier
        // for us to correlate it with other data, and this is why can set
        // the log_msg() to ERROR instead of WARNING, so it is distinctive

        val abs_val_diff_in_epochs = -diff_in_epochs
        if (abs_val_diff_in_epochs > max_age_tolerance_in_time) {
          log_msg(ERROR, "Ignoring: date-time in 5th field is ahead of us " +
                         "too much: " + abs_val_diff_in_epochs +
                         " millisecs: current epoch: " + current_epoch +
                         " epoch of sample: " + epoch_of_measure_in_line +
                         " sample line: " + data_line)
          return null
        } else {
          // just give a notice about the time of this sample but don't return
          // from this function
          log_msg(NOTICE, "a sample has date-time in 5th field ahead of " +
                          "us by " + abs_val_diff_in_epochs +
                          " millisecs: " + data_line)
        }
    }

    // the seventh field is the list of coordinates to which this speed
    // applies. Second argument to boolean "false" because the NYC
    // Traffic Speed OpenData is not in the same "Longitude Latitude"
    // format as the NYC LION Street Segment GeoDB, but in the opposite
    // format, "Latitude Longitude"

    val geometry_inst = StringToGeom(line_values(6), false)
    if (geometry_inst == null) {
      // This field didn't seem to have a valid, comma-separated, list
      // of coordinates: then ignore this line in the CSV file
      log_msg(WARNING, "Ignoring: wrong geometry in 7th field: " +
                       data_line)
      return null
    }

    return data_line

  } // method transformCsvDataLine

} // class ConvertTrafficSpeedCsvUrlToWekaSerializedInsts

