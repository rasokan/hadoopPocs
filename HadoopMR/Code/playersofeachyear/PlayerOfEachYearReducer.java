package com.learn.hadoop.mapreduce.olympicathletespoc.playersofeachyear;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PlayerOfEachYearReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	
	
	private static final Logger log = Logger
			.getLogger(PlayerOfEachYearInvoker.class.getName());
	
	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		log.info("Executing reduce function");
		
		for(Text value:values){
			
			context.write(key, value);
		}

	}

}
