package com.learn.hadoop.mapreduce.olympicathletespoc.countplayerofeachyear;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountPlayerOFEachYearReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {

	private static final Logger log = Logger
			.getLogger(CountPlayerOFEachYearReducer.class.getName());

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		log.info("Executing reduce function");

		Integer sum = 0;

		for (Text value : values) {

			sum = sum + 1;

		}

		context.write(key, new Text(sum.toString()));
	}
}
