package src.main.scala.model

import _root_.java.io.File
import _root_.java.net.URLEncoder

import scala.collection.mutable.ListBuffer

import weka.core.converters.SerializedInstancesLoader

import com.vividsolutions._
import com.vividsolutions.jts.operation.distance.DistanceOp

import org.geoscript.geometry._

import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS


import src.main.scala.logging.Logging._
import src.main.scala.config.Config
import src.main.scala.utils.download.DownloadUrlToFile
import src.main.scala.types.SpeedInPolygonalSection
import src.main.scala.utils.conversion.StringToGeom


class CurrentRealTimeTrafficSpeeds(val weka_bsi_fname: String) {

  var speeds_in_subpolygons: List[SpeedInPolygonalSection] = null

  private [this] val coordRefSysWGS84 = CRS.decode("EPSG:4326")

  /*
   * method: printPolygonalZoneInNYC
   *
   * Prints a polygonal zone in NYC.
   * Currently it uses the Google Maps, but its purpose is to use the New
   * York City Open Data shapefiles, from GIS. (Like the Dept of City
   * Planning LION Single-Line Street Segments File GeoDB, or, e.g.,
   *
   *  https://data.cityofnewyork.us/Housing-Development/Building-Footprints/tb92-6tj8
   *
   * although this one has too much information, since it even has the
   * building foot-prints.) When the code use shapefiles instead of Google
   * Maps, its "color" parameter below will be substituted by a GeoTools'
   * "Style" object, which is richer.
   */

  protected def printPolygonalZoneInNYC(
                                polyg_zone: SpeedInPolygonalSection,
                                color: String, dest_png: String
                        ) {

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
    DownloadUrlToFile(gmap_url, gmap_png)

  } // printPolygonalZoneInNYC()


  /*
   * method: printSpeedVibrancyMap
   *
   * Prints a map with all the polygonal sub-sections of New York City
   * in different colors according to the current real-time speed in
   * its polygonal sub-sections
   *
   * It receives the list of polygons (: List[SpeedInPolygonalSection])
   * and also the min_speed among that list, because that polygonal zone(s)
   * with currently the slowest speed in traffic must be filled in red, and
   * not that zone with traffic speed 0.0, which is pbby won't appear.
   *
   * Similar as function above, this should use GIS from Open Data as not
   * to require API registration keys as Google Maps. The same issue occurs
   * with Open Street Maps, it also requires an API key to support multiple
   * layers of visualization (the polygonal zones of traffic in NYC), but
   * we want to run without API keys:
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

  protected def printSpeedVibrancyMap(min_speed: Double,
                                 polygons: List[SpeedInPolygonalSection],
                                 dest_png: String
                         ) {

    var gmap_paths = new StringBuilder(32*1024)

    /* We order the polygonal zones by speed and take the slowest 12
     * zones because Google Maps would not return more than 12
     * polylines, ie., with 14 polylines it may succeed or it may fail
     */
    for (polygon_zone <- polygons.sortBy(_.speed).take(12)) {
      val polyg: String = URLEncoder.encode(
                               polygon_zone.polygon_encoded, "UTF-8"
                            ).replace("+", "%20")
      // Find which color according to the traffic speed to print this zone in
      // New York City: green is the best, red is slowest
      var color : Long = 0x00FF00
      if (polygon_zone.speed >= Config.nycTargetOptimumTraffSpeed)
        color = 0x00FF00
      else {
        val delta_speed = (polygon_zone.speed - min_speed) /
                            (Config.nycTargetOptimumTraffSpeed - min_speed)

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
    DownloadUrlToFile(gmap_url, gmap_png)

  } // printSpeedVibrancyMap()


  /*
   * method: loadTrafficSpeedPerPolygonInTheCity
   *
   * Loads a WEKA SerializedInstances file with the real-time traffic in
   * NYC and filters those records which have valid polygonal sections for
   * GeoScript.
   * Note: the NYC traffic has the polygons as poly-lines and also in
   * encoded format:
   * https://developers.google.com/maps/documentation/utilities/polylinealgorithm
   *
   * The first format of the polygon is used to validate it, the second to
   * print it in Google Maps.
   *
   * @param weka_bsi_file the filename of the WEKA binary serialized
   *                      instances to load
   * @return the loaded list of polygons real-time speed
   */

  def loadTrafficSpeedPerPolygonInTheCity()
                              : List[SpeedInPolygonalSection] = {

    // load the New York City Real-Time traffic speed loaded from
    //     http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt
    // into GeoScript/Java Topology Suite polygonal sections of New York

    var sinst_in: SerializedInstancesLoader =
                                          new SerializedInstancesLoader()
    sinst_in.setSource(new File(weka_bsi_fname));

    // Get list of valid polygons (OpenData also gives invalid ones)
    var current_valid_polygons =
                              new ListBuffer[SpeedInPolygonalSection]()

    // Record the two locations which have the slowest and fastest speeds
    var min_speed_zone = new SpeedInPolygonalSection(999999.0, null,
                                                       "", null, "")

    var max_speed_zone = new SpeedInPolygonalSection(-1.0, null,
                                                       "", null, "")

    for (instance <- Iterator.continually(sinst_in.getNextInstance(null)).
                                 takeWhile(_ != null)) {

      // Split the fields of this WEKA instance:
      //     The 7th column is the polygonal section inside New York City
      val polygon_str = instance.stringValue(6)
      //     The 8th column in the WEKA instance is theencoded-polygonal by
      //     the Google polyline algorithm format, below:
      // https://developers.google.com/maps/documentation/utilities/polylinealgorithm
      val polygon_enc = instance.stringValue(7)
      // The 2nd column is the traffic speed in that polygonal section
      val speed = instance.value(1)
      //     The 13th column is a well-known address inside New York
      val well_known_address = instance.stringValue(12)

      log_msg(DEBUG, "speed is: " + speed +
                     "\n     inside polygonal section " + polygon_str +
                     "\n     reference point: '" + well_known_address + "'")

      val geometry_instance = StringToGeom(polygon_str)

      if (geometry_instance != null)  {
        // Find the centroid point of this geometry
        val centroid = geometry_instance.centroid
        log_msg(DEBUG, "Centroid is " + centroid)
        current_valid_polygons += new SpeedInPolygonalSection(
                                             speed,
                                             geometry_instance,
                                             polygon_enc,
                                             centroid,
                                             well_known_address)

        // See if this is the location with the less speed so far
        if (speed < min_speed_zone.speed) {
              min_speed_zone = new SpeedInPolygonalSection(
                                             speed,
                                             geometry_instance,
                                             polygon_enc,
                                             centroid,
                                             well_known_address)
        }
        if (speed > max_speed_zone.speed) {
              max_speed_zone = new SpeedInPolygonalSection(
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
    // Print those polygonal subsections of New York City that happen to
    // have now the slowest and the fastest speeds
    if (min_speed_zone.polygon_encoded != "") {
      printPolygonalZoneInNYC(min_speed_zone, "00FFFF",
                              "visualization_slowest_speed.png")
      println("Map of zone with the slowest speed at this moment: " +
              min_speed_zone.speed +
              " (" + min_speed_zone.well_known_addr + ")\n" +
              "Downloaded to " + "visualization_slowest_speed.png")
    }
    if (max_speed_zone.polygon_encoded != "") {
      printPolygonalZoneInNYC(max_speed_zone, "FF00FF",
                              "visualization_fastest_speed.png")
      println("Map of zone with the fastest speed at this moment: " +
              max_speed_zone.speed +
              " (" + max_speed_zone.well_known_addr + ")\n" +
              "Downloaded to " + "visualization_fastest_speed.png")
    }
    if (min_speed_zone.polygon_encoded != "") {

      val list_valid_polyg = current_valid_polygons.toList

      printSpeedVibrancyMap(min_speed_zone.speed, list_valid_polyg,
                            "visualization_vibrancy_speed.png")
      println("Map of vibrancy at this moment downloaded to: " +
              "visualization_vibrancy_speed.png")

      speeds_in_subpolygons = list_valid_polyg
      return list_valid_polyg
    }
    return null
  }  // loadTrafficSpeedPerPolygonInTheCity()


  /**
    * method: minDistanceBetweenTwoGeometries
    *
    * Calculates the minimum distance in meters between two geometries
    *
    * @param g0 the first geometry
    * @param g1 the second geometry
    * @return their distance in meters according to the WGS84 coord-ref-syst
    */

  protected def minDistanceBetweenTwoGeometries(g0: jts.geom.Geometry,
                                               g1: jts.geom.Geometry):
    Double = {

      // get the closest coordinates between the two geometries
      val closestCoords = DistanceOp.closestPoints(g0, g1)

      // calculate the distance in meters between those closest coords
      val distance_meters = JTS.orthodromicDistance(closestCoords(0),
                                                    closestCoords(1),
                                                    coordRefSysWGS84)

      distance_meters
  } // method minDistanceBetweenTwoGeometries(...)


  /**
    * method: centroidDistanceBetweenTwoGeometries
    *
    * Calculates the distance in meters between two points (centroids)
    *
    * @param centroid0 the point of the first centroid
    * @param centroid1 the point of the seconds centroid
    * @return their distance in meters according to the WGS84 coord-ref-syst
    */

  protected def centroidDistanceBetweenTwoGeometries(centroid0: Point,
                                                    centroid1: Point):
    Double = {

      val coord_0 = centroid0.getCoordinate
      val coord_1 = centroid1.getCoordinate
      val distance_meters = JTS.orthodromicDistance(coord_0, coord_1,
                                                    coordRefSysWGS84)

      distance_meters
  } // method centroidDistanceBetweenTwoGeometries(...)


  /**
    * method: findTimeToGoFromOnePolygonToAnother
    *
    * Find the time to go from one polygon to another, based on:
    *
    *   time = ( distance-between-polygons / speed-in-source-polygon )
    *
    * Ie., supposing that we "annotate" by a sign certain hypothetical
    * vehicle in traffic at the source polygon, at what time does it
    * will arrive at the destination polygon.
    *
    * This method needs to be improved, since it is meaningful for the
    * traffic.
    *
    * @param src the source polygonal section in the City where the
    *            "annotated" vehicle departs towards the destination
    *            polygonal section in the City
    * @param dest the destination polygonal section in the City where the
    *             "annotated" vehicle wants to arrive
    * @return the time in seconds the "annotated" vehicle takes
    */

  def findTimeToGoFromOnePolygonToAnother(
                                       src: SpeedInPolygonalSection,
                                       dest: SpeedInPolygonalSection
                                    ):
    Double = {

      // IN-PROGRESS: it is using so far
      //          centroidDistanceBetweenTwoGeometries(...)
      // but it can use as well
      //          minDistanceBetweenTwoGeometries(...)
      // with different results, of course

      val distance_centroids =
        centroidDistanceBetweenTwoGeometries(src.centroid, dest.centroid)

      // assume the speed is in KM/H (the "distance_centroids" is in meters)
      val time_centroids_secs = distance_centroids / ( src.speed / 3.6 )

      time_centroids_secs
  } // method findTimeToGoFromOnePolygonToAnother(...)

}
