# DataMining NewYork Traffic Speed

Some Data Mining with WEKA of NewYork City Traffic Speed
( https://data.cityofnewyork.us/Transportation/Real-Time-Traffic-Speed-Data/xsat-x5sa )

# WIP

This project is a *work in progress*. The implementation is *incomplete* and
subject to change. The documentation can be inaccurate.

# Description

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

