
package src.main.scala.config

/*
 * object: Config
 *
 * It has all the configuration settings
 */

object Config {

      // URL for the New York City Real-Time Link (Traffic) Speed (CSV format)

      val NYC_Traffic_Speed_URL =
                "http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt"

      val Final_LinkSpeed_WEKA_SerInsts_fname =
                                  "New_York_City_Link_Speed.bsi"

      /* This is the latest Traffic Volume Counts per each segment of each
       * street and per-hour of the day in New York City. This URL doesn't
       * have real-time data, the latest sample of volume traffic was taken
       * in 2013-2012.
       *
       * The master URL is:
       *
       * https://data.cityofnewyork.us/NYC-BigApps/Traffic-Volume-Counts-2012-2013-/p424-amsu
       *
       * where several formats are exported, like XML and JSON -which has a
       * good description of the fields in this dataset:
       *
       * https://data.cityofnewyork.us/api/views/p424-amsu/rows.json?accessType=DOWNLOAD
       *
       * but for current versions of WEKA, the CSV format is easier to read.
       */

       val NYC_Traffic_Volume_Count_URL =
         "https://data.cityofnewyork.us/api/views/p424-amsu/rows.csv?accessType=DOWNLOAD"

      val Final_TrafficVolumeCnt_WEKA_SerInsts_fname =
                                  "New_York_City_Traffic_Volume_Count.bsi"


       /* The way to join both URLs, the Real-Time Link (Traffic) Speed -which
        * uses polygonal coordinates of sub-sections in the New York City- and
        * and the Traffic Volume Counts -which uses segments IDs of each part
        * of streets in NYC- is necessary to employ the Department of City
        * Planning's LION Single Line Street Base Map,
        *
        *     https://data.cityofnewyork.us/City-Government/LION/2v4z-66xt
        *
        *     http://www.nyc.gov/html/dcp/download/bytes/nyc_lion15b.zip
        *
        * which gives the polygonal coordinates of each segment ID, and then,
        * with this polygonal coordinates, to do a polygon intersection
        * between both polygons (Traffic Volume Counts' versus Real-Time Link
        * (Traffic) Speed's) using the GeoScript/GeoTools/JTS libraries.
        *
        * This is an example of the LION file as a master data:
        *
        *   http://www.nyc.gov/html/dot/html/motorist/truck_route_nyc_metadata.html
        *
        * The sub-project:
        *
        *   https://github.com/je-nunez/Querying_NYC_Single_Line_Street_Base
        *
        * is an example of using the above libraries with the NYC LION File
        * GeoDataBase. We use this other project for the download and the ETL
        * of the LION GeoDB, since it can be done only once (while the NYC
        * Deparment of City Planning doesn't update the LION GeoDB, which
        * is done only quarterly, according to:
        *     Resource Maintenance -> Update Frequency
        *
        * in:
        *
        *     http://www.nyc.gov/html/dcp/pdf/bytes/lion_metadata.pdf?v=15b
        */

       // Where the ETL left NYC LION import into an ESRI Shapefile:

       val NYC_LION_Polyline_Street_Map =
                "NYC_LION_to_Shapefile/etl_dest_shp/nyc_data_exploration.shp"


       /* Let's say that this is our objective speed, so the New York City can
        * be crossed from extreme to extreme in 15 minutes, at this optimum
        * sustained speed. (Higher speeds than this are ideal and better, of
        * course, but more costly.)
        *
        * This target of the optimum speed can be instead a variable
        * determined at run-time (below the maximum of the real-time speeds of
        * traffic is indeed calculated by the code, but this dinamyc maximum
        * can be either slower than this target optimum, or an outlier value
        * that is too high from the target optimum).
        */

       val target_optimum_NYC_speed: Double = 80.0

       object OpenData_CSV_Parser {
            val NYC_LinkSpeed_Fields_Header = Array(
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

            val NYC_LinkSpeed_CSV_to_WEKA_SerInst = Array(
                                  "-D", "5",
                                  "-format", "M/d/yyyy HH:mm:ss",
                                  "-S", "7-9"
                             )

            val Intermediate_LinkSpeed_clean_CSV =
                                  "New_York_City_Link_Speed.csv"

            val NYC_TrafficVolumeCnts_Fields_Header = Array(
                                                    "ID",
                                                    "Segment ID",
                                                    "Roadway Name",
                                                    "From",
                                                    "To",
                                                    "Direction",
                                                    "Date",
                                                    "12:00-1:00 AM",
                                                    "1:00-2:00AM",
                                                    "2:00-3:00AM",
                                                    "3:00-4:00AM",
                                                    "4:00-5:00AM",
                                                    "5:00-6:00AM",
                                                    "6:00-7:00AM",
                                                    "7:00-8:00AM",
                                                    "8:00-9:00AM",
                                                    "9:00-10:00AM",
                                                    "10:00-11:00AM",
                                                    "11:00-12:00PM",
                                                    "12:00-1:00PM",
                                                    "1:00-2:00PM",
                                                    "2:00-3:00PM",
                                                    "3:00-4:00PM",
                                                    "4:00-5:00PM",
                                                    "5:00-6:00PM",
                                                    "6:00-7:00PM",
                                                    "7:00-8:00PM",
                                                    "8:00-9:00PM",
                                                    "9:00-10:00PM",
                                                    "10:00-11:00PM",
                                                    "11:00-12:00AM"
                                                   )

            val NYC_TrafficVolumeCnt_CSV_to_WEKA_SerInst = Array(
                                  "-D", "7",
                                  "-format", "M/d/yyyy"
                             )

            val Intermediate_TrafficVolumeCnt_clean_CSV =
                                  "New_York_City_Traffic_Volume_Cnt.csv"
       }

} // end of object Config

