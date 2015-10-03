# DataMining NewYork Traffic Speed

Some Data Mining with WEKA of NewYork City Traffic Speed
( https://data.cityofnewyork.us/Transportation/Real-Time-Traffic-Speed-Data/xsat-x5sa )

# WIP

This project is a *work in progress*. The implementation is *incomplete* and
subject to change. The documentation can be inaccurate.

# Description

As **Jane Jacobs** wrote in her book *The Death and Life of Great
American Cities* on **Urban Planning**, in chapter 7, *The Generators
of Diversity*: *To understand cities, we have to deal outright with
combinations or mixtures of uses, not separate uses, as the essential
phenomena... A mixture of uses, if it is to be sufficiently complex
to sustain city safety, public contact and cross-use, needs an enormous
diversity of ingredients. So the first question -and by far the most
important question- about planning cities is this: How can cities
generate enough mixture among uses -enough diversity- throughout
enough of their territories, to sustain their own civilization? It
is all very well to castigate the Great Blight of Dullness and to
understand why it is destructive to city life, but in itself this
does not get us far.*

Her analysis invites to Data Mining in Urban Planning.

By the `Open Data` initiative, the `City of New York's Department of
Transportation`:

     http://nyctmc.org

provides real-time traffic information. E.g.:

     http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt

As of June 26, 2015, this URL can give these columns with information
about the speed of traffic:

      Id numeric
      Speed numeric
      TravelTime numeric
      Borough nominal
      AddressName string

as well as other columns.

`WEKA` ((c) The University of Waikato, Hamilton, New Zealand) is a collection
of machine learning algorithms for data mining tasks. For a general
description of WEKA, its use in Big Data (as in map-reduce jobs to Hadoop),
etc, see:

     http://www.cs.waikato.ac.nz/ml/weka/

as well as other repositories in this account:

     https://github.com/je-nunez/WEKA
     https://github.com/je-nunez/Using_WEKA_in_Scala

This repository is to work with WEKA on the Open Data provided by the City of
New York.

# A sub-project

This project uses code and results from this other sub-project that does an
ETL on `the New York City's Department of City Planning LION Single-line
Street` database (which changes at most once quarterly) to a custom -and
smaller- `ESRI Shapefile`:

    https://github.com/je-nunez/Querying_NYC_Single_Line_Street_Base

This project refers to this other sub-project as a git-submodule, so you may
need to:

     git clone --recurse-submodules  <git-to-this-repository>

to clone also that sub-project.

# Required Libraries

The New York City Real-Time Traffic Speed URL,

     http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt

divides the speed of traffic in polygons. To analyze these polygons, and their
operations, this project needs the GeoScript library:

     http://geoscript.org/

like

     http://geoscript.org/examples/geom/relate.html#intersection

You need to install the `GeoScript Scala` library:

     http://geoscript.org/scala/quickstart.html

and its underlying libraries, the `Java Topology Suite` (JTS):

    http://www.vividsolutions.com/jts/JTSHome.htm

and the `Open Source Java GIS Toolkit` from the `Open Source
Geospatial Foundation`:

    http://sourceforge.net/projects/geotools/files/

The objective is to use `GIS` with the New York City `shapefiles` from
the `BYTES of the BIG APPLE` web-site:

    http://www.nyc.gov/html/dcp/html/bytes/districts_download_metadata.shtml#cbt

or, in general, a shapefile that *happens to be more precise for the actual
polygons offered by the NYC Department of Transportation at its first URL above*
(ie., the shapefiles in this link in the `BYTES of the BIG APPLE` web-site
might be approximations for the NYC Dept. of Transportation's polygonal
sections of the city). Ie., other possible shapefiles are the very good
explanation here:

    http://docs.geoserver.org/latest/en/user/gettingstarted/shapefile-quickstart/index.html

or:

    https://data.cityofnewyork.us/Housing-Development/Building-Footprints/tb92-6tj8

or the underlying shapefile in, for example,

    http://maps.nyc.gov/doitt/nycitymap/?z=7&p=990013,189733&f=SUBWAY

or other polygonal section the URL of the NYC Dept. of Transportation contains.
So far, for simplicity, we use a URL to Google Maps.

# A very first version of the visualization

This is a very first version of the visualization of the real-time
traffic speed in New York City. (This version uses Google Maps, although
we intend to replace it with plotting directly with the ESRI Shapefiles
of New York City -mainly from the Department of City Planning LION GeoDB
Single Line Street Base-, for Google Maps, Open Street Maps, etc., require
an API key to query their services for complex, general cases, and we
want to avoid asking for an API key for this program, but working directly
with the ESRI shapefiles, which further give other information attached to
each geo-location, so it is better for Data Mining).

In this map, there are some sections of traffic with a shadow: in them, the
speed happened to be slower in that real-time sample of the traffic:

![sample New York City real-time traffic speed](/visualiz_traffic_vibrancy_speed_a_first_sample.png?raw=true "sample New York City real-time traffic speed")

# Required Environment Variables

These are the required environment variables:

     # build the JVM CLASSPATH we need
      
     # substitute "<path-to>" below by the real path where each is located
     V=""     # variable V will have the CLASSPATH we need
     for p in <path-to>/weka.jar  \
              <path-to>/geotools-12.4/jts-1.13.jar \
              <path-to>/geotools-12.4/gt-opengis-12.4.jar \
              <path-to>/geotools-12.4/gt-api-12.4.jar  \
              <path-to>/geotools-12.4/gt-render-12.4.jar \
              <path-to>/geoscript.scala/geoscript/target/scala-2.10/classes
     do
         V="${V}:$p"
     done
      
     export CLASSPATH="$CLASSPATH:${V}"

# Status of this project

So far, the script `NewYorkCity_Link_Speed_Data_Mining.scala` is able to
download the link

     http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt

clean the real-time information inside it (it is real-time information, so some
input records have extraneous characters like `carriage-return`, etc, which
makes an `Extract, Transform and Load ` of the real-time data necessary, since
WEKA would not accept such records), convert these records into a WEKA ARFF
dataset, and load it into WEKA.

The ETL parser in `NewYorkCity_Link_Speed_Data_Mining.scala` is strict, so if
the format given by

     http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt

changes, it will refuse to continue and fail, instead of feeding the unknown
format into WEKA. (This decision could be very conservative.)

(WEKA also offers some input converters, see:

     http://weka.sourceforge.net/doc.dev/weka/core/converters/package-tree.html

like those implementing the interface

     weka.core.converters.URLSourcedLoader
      
     http://weka.sourceforge.net/doc.dev/weka/core/converters/URLSourcedLoader.html

but the real-time information from
`http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt` is not clean,
and some input records need to be transformed or discarded)

One issue with the original ETL approach was that it parsed

     http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt

in only one pass, not two-passes, so it declared some attributes as "strings"
or "numeric" in the WEKA ARFF header (ARFF is only one text file, with only a
header part and a data part, not two files '.names' and '.data' as Ross Quinlan's
C4.5). But the WEKA classifiers prefer attributes declared as "nominal", hence a
two-pass parser is better, and is the one that is implemented now. (The parser
can also be incremental, since WEKA's `ARFFSaver` implements the
`IncrementalConverter` interface -see its `writeIncremental(...)` public method.)

# Current status of this project

There is an issue as of September 2015 for Data Mining with the current Real-Time
Open Data source the City of New York provides:

     http://real2.nyctmc.org/nyc-links-cams/LinkSpeedQuery.txt

While it has the Real-Time speeds updated in less than 2 minutes, it lacks the
direction of traffic. (The current fields of this CSV file are -with an example of
their values for one actual record to the left:

     "Id":  "119"
     "Speed":  "32.93"
     "TravelTime":   "194"
     "Status":   "0"
     "DataAsOf":   "9/29/2015 22:31:24"
     "linkId":   "4456502"
     "linkPoints":   "40.70631,-74.01501 40.705380,-74.01528 40.70496,-74.01546 40.70374,-74.01574001 40.70304,-74.01582 40.70126,-74.01574 40.70026,-74.01541 40.69408,-74.01304 40.68556,-74.00779 40.68363,-74.00668 40.68213,-74.00602 40.681160,-74.005320 40.6798,-74.00416"
     "EncodedPolyLine":   "mmmwFx`wbMxDt@rAb@rFv@jCNbJOfEaAre@yMft@y_@`K}EjHcC`EkCnGgF"
     "EncodedPolyLineLvls":  "BBBBBBBBBBBBB"
     "Owner":   "MTA Bridges & Tunnels"
     "Transcom_id":   "4456502"
     "Borough":    "Manhattan"
     "linkName":   "BBT E Manhattan Portal - Toll Plaza"

so, while the `Speed` field is given, as well as the continuous travel time through the
link-camera, the Open Data CSV as of September 2015 lacks the direction of this speed
of traffic. (Ie., traffic at the same point but in opposite direction can have different
speeds.)

Other projects have been started with a similar objective but using the General Transit
Feed Specification files, taking the frequency of urban public buses given by the GTFS
as probes inside the transit of the speed in that intersection. (GTFS also has the
`shapes.txt` and `stops.txt` files which have coordinates, ie., geometries which consist
in single lines or points, respectively in each file. The Link Speed Query using 
Sensors does show other information that GTFS probably can't give, ie., refreshed in less
than 2 minutes, and it can cover geographical areas where buses may not pass very frequently,
like certain bridges, etc. Besides, in comparison to Link Speed Queries (Sensors), GTFS can
also have its limitations, since there can be segments of the streets where the public
transit uses reserved lanes, so the public transit vehicle doesn't need to maintain in them
the normal traffic speed for not becoming a bottleneck. In summary, measuring the normal
traffic speed using GTFS as probes inside the traffic, and Link Speed Sensors improve
the other' capacities for this problem of traffic space availability and planning.)
