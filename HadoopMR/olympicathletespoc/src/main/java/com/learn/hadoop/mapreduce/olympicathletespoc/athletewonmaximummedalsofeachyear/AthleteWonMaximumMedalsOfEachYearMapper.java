package com.learn.hadoop.mapreduce.olympicathletespoc.athletewonmaximummedalsofeachyear;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AthleteWonMaximumMedalsOfEachYearMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	private static final Log log = LogFactory
			.getLog(AthleteWonMaximumMedalsOfEachYearMapper.class);

	private static LongWritable yearKeyValue = new LongWritable();

	private static final String COMMA = ",";

	private static final Text appendPlayerNameWithMedalCountValue = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		log.info("Executing Map Function");

		String lineRead = value.toString();

		String lineReadValues[] = lineRead.split(",");

		String yearValue = lineReadValues[3];

		Integer totalMedalOfEachPlayer = Integer.parseInt(lineReadValues[9]);

		String playerValue = lineReadValues[0];

		yearKeyValue.set(Long.parseLong(yearValue));

		StringBuffer appendPlayerNameWithMedalCount = new StringBuffer();

		appendPlayerNameWithMedalCount.append(playerValue).append(COMMA)
				.append(totalMedalOfEachPlayer);

		appendPlayerNameWithMedalCountValue.set(appendPlayerNameWithMedalCount
				.toString());

		context.write(yearKeyValue, appendPlayerNameWithMedalCountValue);
	}
}
