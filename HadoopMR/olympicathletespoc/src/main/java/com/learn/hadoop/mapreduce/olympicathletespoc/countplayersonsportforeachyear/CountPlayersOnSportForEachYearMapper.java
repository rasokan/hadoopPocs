package com.learn.hadoop.mapreduce.olympicathletespoc.countplayersonsportforeachyear;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountPlayersOnSportForEachYearMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	private static final Log log = LogFactory
			.getLog(CountPlayersOnSportForEachYearMapper.class);

	private static final Text countryNameAndPlayerNameValue = new Text();

	private static final String COMMA = ",";

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		log.info("Executing Map Function");

		String lineRead = value.toString();

		String lineReadValues[] = lineRead.split(",");

		Long yearValue = Long.parseLong(lineReadValues[3]);

		String sportsNameValue = lineReadValues[5];

		String playerNameValue = lineReadValues[0];

		StringBuffer appendCountryNameAndPlayerNameValue = new StringBuffer();

		appendCountryNameAndPlayerNameValue.append(playerNameValue)
				.append(COMMA).append(sportsNameValue);

		countryNameAndPlayerNameValue.set(appendCountryNameAndPlayerNameValue
				.toString());

		context.write(new LongWritable(yearValue),
				countryNameAndPlayerNameValue);

	}

}
