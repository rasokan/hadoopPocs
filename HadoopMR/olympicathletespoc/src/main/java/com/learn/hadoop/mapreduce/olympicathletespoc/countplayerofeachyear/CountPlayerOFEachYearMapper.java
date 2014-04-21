package com.learn.hadoop.mapreduce.olympicathletespoc.countplayerofeachyear;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountPlayerOFEachYearMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	private static final LongWritable keyYearValue = new LongWritable();

	private static final Text playerNameValueText = new Text();

	private static final Logger log = Logger
			.getLogger(CountPlayerOFEachYearMapper.class.getName());

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		log.info("Executing Map Function");
		
		String lineRead = value.toString();

		String lineReadValues[] = lineRead.split(",");

		String yearValue = lineReadValues[3];

		String playerNameValue = lineReadValues[0];

		keyYearValue.set(Long.parseLong(yearValue));

		playerNameValueText.set(playerNameValue);

		context.write(keyYearValue, playerNameValueText);

	}

}
