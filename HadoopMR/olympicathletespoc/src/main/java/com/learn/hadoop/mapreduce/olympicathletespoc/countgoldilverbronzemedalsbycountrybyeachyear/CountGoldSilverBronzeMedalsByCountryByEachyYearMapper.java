package com.learn.hadoop.mapreduce.olympicathletespoc.countgoldilverbronzemedalsbycountrybyeachyear;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountGoldSilverBronzeMedalsByCountryByEachyYearMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	private static final Log log = LogFactory
			.getLog(CountGoldSilverBronzeMedalsByCountryByEachyYearMapper.class);

	private static LongWritable yearKeyValue = new LongWritable();

	private static final String COMMA = ",";

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		log.debug("Executing map function");

		String lineRead = value.toString();

		String lineReadValues[] = lineRead.split(",");

		String yearValue = lineReadValues[3];

		String countryValue = lineReadValues[2];

		Integer goldMedalValue = Integer.parseInt(lineReadValues[6]);

		Integer silverMedalValue = Integer.parseInt(lineReadValues[7]);

		Integer bronzeMedalValue = Integer.parseInt(lineReadValues[8]);

		StringBuffer resultToWrite = new StringBuffer();

		resultToWrite.append(countryValue).append(COMMA).append(goldMedalValue)
				.append(COMMA).append(silverMedalValue).append(COMMA)
				.append(bronzeMedalValue);

		yearKeyValue.set(Long.parseLong(yearValue));

		context.write(yearKeyValue, new Text(resultToWrite.toString()));
	}

}
