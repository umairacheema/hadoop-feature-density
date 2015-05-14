package com.umaircheema.sbda;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregationReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context ctx)
			throws IOException, InterruptedException {

		int sumCount = 0;
		// Sum the count of Points within Polygon boundaries

		for (IntWritable sum : values) {
			sumCount += sum.get();
		}

		ctx.write(key, new IntWritable(sumCount));
	}

}
