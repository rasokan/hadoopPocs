package com.learn.hadoop.mapreduce.olympicathletespoc.athletewonmaximummedalsofeachyear;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AthleteWonMaximumMedalsOfEachYearReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {

	private static final Log log = LogFactory
			.getLog(AthleteWonMaximumMedalsOfEachYearReducer.class);

	private static final String WHITE_SPACE = "		";

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		log.info("Executing reduce function");

		Integer minValue = Integer.MIN_VALUE;

		Integer maxMedalCountValue = 0;

		String maxPlayerNameValue = null;

		Integer medalCountValue = 0;

		for (Text value : values) {

			String playerNameAndMedalValueString[] = value.toString()
					.split(",");

			String playerNameValue = playerNameAndMedalValueString[0];

			medalCountValue = Integer
					.parseInt(playerNameAndMedalValueString[1]);

			if (medalCountValue >= maxMedalCountValue
					&& medalCountValue >= minValue) {

				maxMedalCountValue = medalCountValue;

				maxPlayerNameValue = playerNameValue;
			}
		}

		StringBuffer resultToWrite = new StringBuffer();

		resultToWrite.append("Player  Name:").append(maxPlayerNameValue)
				.append(WHITE_SPACE).append("Medal Count of the Player:")
				.append(maxMedalCountValue);

		context.write(key, new Text(resultToWrite.toString()));
	}
}
