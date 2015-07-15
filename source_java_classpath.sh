
# fill in the paths "<...path-to...>" below, and then source this file into
# your shell session (sh or bash: for other shells, the idea is similar)

# build the JVM CLASSPATH we need: most of the components below are Java JAR
# files, except GeoScript, which is the base of the GeoScript Scala
# ./classess/ compiled directory


   # http://www.cs.waikato.ac.nz/ml/weka/downloading.html

WEKA_JAR="<...path-to...>/weka.jar"


   # http://www.vividsolutions.com/jts/download.htm
   # (We're giving here the authoritative locations to download. There are
   # replicas, like
   # http://repo.maven.apache.org/maven2/com/vividsolutions/jts/1.13/jts-1.13.jar

JTS_JAR="<...path-to...>/geotools-12.4/jts-1.13.jar"


   # http://sourceforge.net/projects/geotools/files/

GT_OPENGIS_JAR="<...path-to...>/geotools-12.4/gt-opengis-12.4.jar"

GT_API_JAR="<...path-to...>/geotools-12.4/gt-api-12.4.jar"

GT_RENDER_JAR="<...path-to...>/geotools-12.4/gt-render-12.4.jar"


   # http://geoscript.org/scala/download.html

SCALA_GEOSCRIPT_DIRECT_TREE=\
     "<...path-to...>/geoscript.scala/geoscript/target/scala-2.10/classes"


# Summarize the above in two lists, one of JARs and another of dir-trees

LIST_JARs="$WEKA_JAR:$JTS_JAR:$GT_OPENGIS_JAR:$GT_API_JAR:$GT_RENDER_JAR"
LIST_DIR_TREES="$SCALA_GEOSCRIPT_DIRECT_TREE"


# Build the JVM CLASSPATH

CLASSPATH="$CLASSPATH:$LIST_JARs:$LIST_DIR_TREES"

export CLASSPATH

