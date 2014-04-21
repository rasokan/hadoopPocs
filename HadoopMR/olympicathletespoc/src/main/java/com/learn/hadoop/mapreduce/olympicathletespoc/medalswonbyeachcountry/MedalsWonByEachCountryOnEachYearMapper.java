package com.learn.hadoop.mapreduce.olympicathletespoc.medalswonbyeachcountry;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedalsWonByEachCountryOnEachYearMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	private static final Log log = LogFactory
			.getLog(MedalsWonByEachCountryOnEachYearMapper.class);

	private static LongWritable yearKeyValue = new LongWritable();

	private static Text appendYearAndMedalOfEachPlayerValue = new Text();

	private static final String COMMA = ",";

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		log.info("Executing Map Function");

		String lineRead = value.toString();

		String lineReadValues[] = lineRead.split(",");

		String countryValue = lineReadValues[2];

		String yearValue = lineReadValues[3];

		String totalMedalOfEachPlayer = lineReadValues[9];

		yearKeyValue.set(Long.parseLong(yearValue));

		StringBuffer appendYearAndMedalOfEachPlayer = new StringBuffer();

		appendYearAndMedalOfEachPlayer.append(countryValue).append(COMMA)
				.append(totalMedalOfEachPlayer);

		appendYearAndMedalOfEachPlayerValue.set(appendYearAndMedalOfEachPlayer
				.toString());

		context.write(yearKeyValue, appendYearAndMedalOfEachPlayerValue);

	}

}
