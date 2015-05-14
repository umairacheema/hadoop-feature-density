#!/bin/bash
# Default values
 KEY_ATTRIBUTE="NAME"
 LONGITUDE_INDEX=1
 LATITUDE_INDEX=2
 FILTER_INDEX=3
 FILTER_TEXT=" "
#Print help
function usage(){
 echo " "
 echo " Usage: featuredensity <KEY ATTRIBUTE> <LONGITUDE INDEX> <LATITUDE INDEX> <FILTER COLUMN INDEX> <FILTER TEXT>"
 echo "  Where <KEY ATTRIBUTE>   : is the field in the Polygons on which data is to be aggregated"
 echo "        <LONGITUDE INDEX> : is the index for longitude in the input CSV data"
 echo "        <LATITUDE INDEX> : is the index for latitude in the input CSV data"
 echo "        <FILTER COLUMN INDEX> : is the index of column on which a Map filter will be applied:Use -1 to include all"
 echo "        <FILTER TEXT>: is the text that if found in filter column will ensure inclusion by Map Task:User -1 to include all"
 echo " "
 echo " "
 echo " Example:"
 echo "        ./featuredensity name 1 2 3 hotel"
 echo "      "
 echo "          Features in the CSV file will be aggregated using <name> as attribute "
 echo "          Longitude coordinates are in the first column"
 echo "          Latitudee coordinates are in the second column"
 echo "          Map Task will only include if in third column values contain the word hotel"
 echo " IMPORTANT: Examine polygons.json in the data/polygons directory to find Key Attribute"
 echo "            and examine points.csv in the data/points directory to find the latitude and longitude column indexes."
 echo " "
 echo " "
 echo " "
 exit 0 
}

#Check inout arguments
if [ "$#" -ne 5 ]; then
    usage
fi
#The first argument is the Key Attribute on which aggreation takes place
      KEY_ATTRIBUTE=$1
# The second argument is the column index of longitude in CSV file
     LONGITUDE_INDEX=$2 
# The third argument is the column index of latitude in CSV file
    LATITUDE_INDEX=$3 
 # The fourth argument is the column index in CSV file on which filter is to be applied
 FILTER_INDEX=$4
 # The fifth argument is the text that needs to be matched by Map task filter
 FILTER_TEXT=$5

# Hadoop Cluster configuration
NAME_NODE_URL=hdfs:/localhost:8020
JOB_TRACKER_URL=localhost:8021
OSM_DIR=/sbda

JOB_DIR=$OSM_DIR/job
LIB_DIR=$OSM_DIR/lib
DATA_DIR=$OSM_DIR/data
OUTPUT_DIR=$OSM_DIR/output
DASHBOARD_DIR=./Dashboard

#Remove previous FeatureDensity
rm -rf FeatureDensity.jar

#Copy latest version from target
cp ./target/FeatureDensity-0.0.1-SNAPSHOT-jar-with-dependencies.jar FeatureDensity.jar

#Remove previous hdfs directory
hadoop fs -rm -r $OSM_DIR

echo "* Creating HDFS directories"
hadoop fs -mkdir $OSM_DIR $DATA_DIR $DATA_DIR/polygons $DATA_DIR/points

echo "* Copying input data to HDFS"
hadoop fs -put data/polygons/* $DATA_DIR/polygons
hadoop fs -put data/points/* $DATA_DIR/points
echo "* Copying input data to Dashboards"
cp data/polygons/polygons.json  $DASHBOARD_DIR/data.json
echo "* Expecting Longitude Column Index in CSV file at $LONGITUDE_INDEX"
echo "* Expecting Latitude Column Index in CSV file at $LATITUDE_INDEX"

echo "* Executing MapReduce job"
hadoop jar FeatureDensity.jar \
           hdfs://$DATA_DIR/polygons/polygons.json \
	   $KEY_ATTRIBUTE \
	   hdfs://$DATA_DIR/points/points.csv \
           $LONGITUDE_INDEX \
	   $LATITUDE_INDEX \
           $FILTER_INDEX \
           $FILTER_TEXT \
	   hdfs://$OUTPUT_DIR


rm results.txt

echo "* Copying results from HDFS to local filesystem"
hadoop fs -getmerge $OUTPUT_DIR results.txt
echo "*Copying results to Dashboard"
cp results.txt $DASHBOARD_DIR/.
echo "* Printing results"
cat results.txt
echo "* Preparing Dashboards Launch URL"
echo " "
echo " "
echo " Please run the following command on the terminal to launch Dashboards"
echo " "
echo "nohup firefox Dashboard/index.html?attribute=$KEY_ATTRIBUTE\&format=geojson&"
echo "  "
echo "  "
echo "  "

