import sys.process._
import _root_.java.net.URL
import _root_.java.io.File
import _root_.java.util.regex._
import _root_.java.text.SimpleDateFormat


val NYC_Traffic_Speed_URL = "http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt"


def write_WEKA_attribute_header(header_line: String,
                                arff_file: java.io.FileWriter) {

      var attributes: Array[java.lang.String] = header_line.
                                                  replaceAllLiterally("\"", "").
                                                  split("\\s+")

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
          // to be changed
          throw new Exception("Format of New York City attributes has changed.")

      // Print the ARFF header to the file
      arff_file.write("@relation New_York_City_Link_Speed\n\n")

      // Print attributes to the ARFF file

      arff_file.write("@attribute Id numeric\n")
      arff_file.write("@attribute Speed numeric\n")
      arff_file.write("@attribute TravelTime numeric\n")
      arff_file.write("@attribute Status numeric\n")
      arff_file.write("@attribute DataAsOf DATE \"M/d/yyy HH:mm:ss\"\n")
      arff_file.write("@attribute linkId string\n")
      arff_file.write("@attribute linkPoints string\n")
      arff_file.write("@attribute EncodedPolyLine string\n")
      arff_file.write("@attribute EncodedPolyLineLvls string\n")
      arff_file.write("@attribute Owner string\n")
      arff_file.write("@attribute Transcom_id string\n")
      arff_file.write("@attribute Borough string\n")
      arff_file.write("@attribute linkName string\n")

      // Write an empty line after the ARFF attributes
      arff_file.write("\n")

      // The ARFF data begins now
      arff_file.write("@data\n")
}

def write_WEKA_data_line(data_line: String, arff_file: java.io.FileWriter) {

      // a better parser is needed of the lines that the
      // New York City Link Speed Query gives us
      var line_values: Array[java.lang.String] = data_line.
                                                 replaceAll("\\\\", "\\\\\\\\").
                                                 stripPrefix("\"").
                                                 stripSuffix("\"").
                                                 split("\"\\s+\"")

      // We expect that the parsing above returned 13 fields in this array

      if(line_values.length != 13) {
          System.err.println("WARNING: Ignoring line: " + data_line)
          return
      }

      val weka_special_chars = Pattern.compile("[ ,]")

      val date_fmt = "M/d/yyyy HH:mm:ss"
      // JODA is better
      val nyc_date_time_fmt = new java.text.SimpleDateFormat(date_fmt)
      nyc_date_time_fmt.setLenient(false)

      var column_number = 1

      for (value <- line_values) {
           val s = value.stripPrefix("\"").stripSuffix("\"")

           // Verify that we have a valid record,
           // by column #5 being a date-time format
           if (column_number == 5) {
               try {
                   val d = nyc_date_time_fmt.parse(s)
               } catch {
                   case e: java.text.ParseException => {
                      System.err.println("WARNING: Ignoring line: " + data_line)
                      return
                   }
              }
           }

           if (s.isEmpty)
               arff_file.write(",")
           else {
               if (column_number > 1)
                   arff_file.write(",")
               arff_file.write("\"" + value + "\"")
               // if (s.indexOf(' ') != -1)
               // if (s.matches(weka_special_chars))
               /*
               if (weka_special_chars.matcher(s).find)
                   arff_file.write("\"" + s + "\"")
               else
                   arff_file.write(s)
               */
           }
           column_number += 1
      }
      // Write the new-line, that is the end of the WEKA record
      arff_file.write("\n")
}

def download_NYC_TrafficSpeed_to_WEKA_arff(src_url: String, dest_file: String) {
      try {
            val src = scala.io.Source.fromURL(src_url)
            val out = new java.io.FileWriter(dest_file)

            var line_number = 1

            for (line <- src.getLines) {
                // out.write(line + "\n")
                if ( line_number == 1 )
                     write_WEKA_attribute_header(line, out)
                else
                     write_WEKA_data_line(line, out)
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


download_NYC_TrafficSpeed_to_WEKA_arff(NYC_Traffic_Speed_URL, "temp_file.arff")


