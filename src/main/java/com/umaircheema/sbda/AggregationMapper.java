package com.umaircheema.sbda;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.QuadTree.QuadTreeIterator;
import com.esri.core.geometry.SpatialReference;
import com.esri.json.EsriFeature;
import com.umaircheema.sbda.util.FeatureFactory;

public class AggregationMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	// Column indices for lat/lon values in the CSV
	int longitudeIndex = 0;
	int latitudeIndex = 1;
	//Column index for text filter
	int filterIndex = -1;
	//Default Filter Text 
	String filterText="@EPSG:4326(WGS84)";
	//Flag to activate text filter
	boolean activateFilter=false;
	// Path for input Polygon data
	String featuresPath;
	// This is the attribute to be used for aggregation
	String labelAttribute;
	List<EsriFeature> featuresList;
	// Spatial Reference (Only WGS84 at present)
	SpatialReference spatialReference;
	QuadTree quadTree;
	QuadTreeIterator quadTreeIter;

	/**
	 * Set up Mapper
	 */
	@Override
	public void setup(Context context) {
		Configuration config = context.getConfiguration();
		spatialReference = SpatialReference.create(4326);

		// Read configuration
		featuresPath = config.get("polygons.features.input");
		labelAttribute = config.get("polygons.features.keyattribute", "NAME");
		longitudeIndex = config.getInt("points.csv.longitude.column", 0);
		latitudeIndex = config.getInt("points.csv.latitude.column", 1);
		filterIndex = config.getInt("points.csv.filter.column", -1);
		filterText = config.get("points.csv.filter.text", filterText);
		// Create WGS84/EPSG:4326 based SpatialReference
		spatialReference = SpatialReference.create(4326);
		//Filter Activation
		if(filterIndex>-1){
			activateFilter=true;
		}
		// Setup Input Stream from HDFS
		FSDataInputStream inputStream = null;

		try {
			// load the ESRI JSON/GeoJSON file provided as argument 0
			FileSystem hdfs = FileSystem.get(config);
			inputStream = hdfs.open(new Path(featuresPath));
			FeatureFactory featureFactory = new FeatureFactory();
			featuresList = featureFactory.parseFeatures(inputStream);
			// Read ESRI JSON
			if (featuresList.isEmpty()) {
				inputStream.close();
				inputStream = hdfs.open(new Path(featuresPath));
				featuresList = featureFactory
						.parseESRIJSONFeatures(inputStream);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
				}
			}
		}

		// build a quadtree of our features for fast queries
		if (featuresList != null) {
			buildQuadTree();
		}
	}

	/**
	 * Map Task Method
	 */
	@Override
	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {

		/*
		 * The TextInputFormat we set in the configuration, by default, splits a
		 * text file line by line. The key is the byte offset to the first
		 * character in the line. The value is the text of the line.
		 */

		String line = val.toString().replaceAll("\"", "");
		String[] fields = line.split(",");
		
		// Get the position of the record
		Point point = getLocationCoordinates(fields);

		if (point != null) {
			// Each map only processes one point feature at a time, so we start
			// out with our count
			// as 1. Aggregation will occur in the combine/reduce stages
			IntWritable one = new IntWritable(1);

			int featureIndex = queryQuadTree(point);

			if (featureIndex >= 0) {
				String name = (String) featuresList.get(featureIndex).attributes
						.get(labelAttribute);

				if (name == null)
					name = "Unknown";
				name = name.replaceAll("\"", "");

				if((activateFilter)&&(filterIndex<fields.length)){
					String attributeValue=fields[filterIndex];
					attributeValue=attributeValue.toLowerCase();
					if(attributeValue.contains(filterText.toLowerCase())){
						context.write(new Text(name), one);
					}
					
				}else{
					context.write(new Text(name), one);
				}
			} else {
				context.write(new Text("[Points Outside Polygons]"), one);
			}
		}
	}

	/**
	 * Build Quad Tree data structure for speeding up Point in polygon test
	 */
	private void buildQuadTree() {
		quadTree = new QuadTree(new Envelope2D(-180, -90, 180, 90), 8);

		Envelope envelope = new Envelope();
		for (int i = 0; i < featuresList.size(); i++) {
			featuresList.get(i).geometry.queryEnvelope(envelope);
			quadTree.insert(i,
					new Envelope2D(envelope.getXMin(), envelope.getYMin(),
							envelope.getXMax(), envelope.getYMax()));
		}

		quadTreeIter = quadTree.getIterator();
	}

	/**
	 * Query the Quad Tree for a hit for a point with specific coordinates
	 * 
	 * @param pt
	 * @return
	 */
	private int queryQuadTree(Point pt) {
		// reset iterator to the quadrant envelope that contains the point
		// passed
		quadTreeIter.resetIterator(pt, 0);

		int elmHandle = quadTreeIter.next();

		while (elmHandle >= 0) {
			int featureIndex = quadTree.getElement(elmHandle);

			// we know the point and this feature are in the same quadrant, but
			// we need to make sure the feature
			// actually contains the point
			if (GeometryEngine.contains(
					featuresList.get(featureIndex).geometry, pt,
					spatialReference)) {
				return featureIndex;
			}

			elmHandle = quadTreeIter.next();
		}

		// feature not found
		return -1;
	}

	/**
	 * Converts latitude and longitude into a Point geometry
	 * 
	 * @param record
	 * @return
	 */
	private Point getLocationCoordinates(String record[]) {
		Point point = null;
		if (record.length < 2)
			return null;
		try {

			Double latitude = Double.parseDouble(record[latitudeIndex]);
			Double longitude = Double.parseDouble(record[longitudeIndex]);
			point = new Point(longitude, latitude);
		} catch (NumberFormatException nfe) {
			return null;
		}
		return point;

	}
}
