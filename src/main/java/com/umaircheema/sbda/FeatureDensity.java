package com.umaircheema.sbda;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FeatureDensity {
	public static int main(String[] init_args) throws Exception {
		Configuration config = new Configuration();

		// To separate arguments needed for mapreduce jobs from all command line
		// arguments
		String[] args = new GenericOptionsParser(config, init_args)
		.getRemainingArgs();

		if (args.length != 8) {
			System.out.println("Invalid Arguments");
			print_usage();
			System.exit(0);
		}

		config.set("polygons.features.input", args[0]);
		config.set("polygons.features.keyattribute", args[1]);
		config.set("mapreduce.output.textoutputformat.separator", ",");
		config.setInt("points.csv.longitude.column", getColumnIndices(args)[0]);
		config.setInt("points.csv.latitude.column", getColumnIndices(args)[1]);
		config.setInt("points.csv.filter.column", Integer.parseInt(args[5])-1);
		config.set("points.csv.filter.text", args[6]);
		Job job = new Job(config);
		job.setJobName("Feature Density");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(AggregationMapper.class);
		job.setReducerClass(AggregationReducer.class);

		// In our case, the combiner is the same as the reducer. This is
		// possible
		// for reducers that are both commutative and associative
		job.setCombinerClass(AggregationReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.setInputPaths(job, new Path(args[2]));
		TextOutputFormat.setOutputPath(job, new Path(args[7]));

		job.setJarByClass(FeatureDensity.class);

		boolean status = job.waitForCompletion(true);

		if (status) {

		} else {
			return 1;
		}

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Returns the Longitude and Latitude Indices
	 * 
	 * @param args
	 * @return
	 */
	private static int[] getColumnIndices(String[] args) {
		int[] indices = { 1, 2 };

		try {
			indices[0] = Integer.parseInt(args[3])-1;
			indices[1] = Integer.parseInt(args[4])-1;
		} catch (NumberFormatException nfe) {
			indices[0] = 1;
			indices[1] = 2;
		}

		return indices;
	}

	/**
	 * Displays help on Usage
	 */
	private static void print_usage() {
		System.out.println("Spatial Big Data Analytics Project");
		System.out.println("FeatureDensity Operation");
		System.out.println("Usage: hadoop jar FeatureDensity.jar   [hdfs path to polygons data] [key attribute for filtering] [hdfs path to points data] [longitude Index] [latitude Index] [hdfs path to output]");
		System.out.println("  [hdfs path to polygons data]  - The polygon data should be in GeoJSON or ESRI JSON format with json as extension");
		System.out.println("  [key attribute from polygon data] - The field name from polygons data on which features count should be computed");
		System.out.println("  [hdfs path to points data]    - CSV file containing input data to be aggregated over polygons");
		System.out.println("  [longitude Index]             - Column index of longitude values in the CSV file, default is 0");
		System.out.println("  [latitude Index]              - Column index of latitude  values in the CSV file, default is 1");
		System.out.println("  [filter  Index]               - Column index of  values in the CSV file that are to be filtered");
		System.out.println("  [filter text]                 - Text that should contain the values of filter index column for inclusion");
		System.out.println("  [hdfs path to output]         - Output file path");

	}
}