package com.learn.hadoop.mapreduce.olympicathletespoc.youngoldathletewongoldmedal;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YoungOldAthleteWonGoldMedalMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	private static final LongWritable keyYearValue = new LongWritable();

	private static final Text playerNameValueTextWithAgeGoldMedal = new Text();

	private static final Log log = LogFactory
			.getLog(YoungOldAthleteWonGoldMedalMapper.class);

	private static final String SEPARATION_CHARACTER = ",";

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		log.debug("Executing Map Function");

		log.info("Executing Map Function");

		System.out.println("Executing Map Function");

		String lineRead = value.toString();

		log.debug("Read lines in map function" + lineRead);

		log.info("Logging Read lines in map function" + lineRead);

		if (!lineRead.isEmpty()) {

			String lineReadValues[] = lineRead.split(",");

			String yearValue = lineReadValues[3];

			String playerNameValue = lineReadValues[0];

			String ageValue = lineReadValues[1];

			String goldMedalValue = lineReadValues[6];

			if (!playerNameValue.isEmpty() && !ageValue.isEmpty()
					&& !goldMedalValue.isEmpty()) {

				StringBuffer appendedAgeandPlayerNameValue = new StringBuffer();

				appendedAgeandPlayerNameValue.append(playerNameValue)
						.append(SEPARATION_CHARACTER).append(ageValue)
						.append(SEPARATION_CHARACTER).append(goldMedalValue);

				keyYearValue.set(Long.parseLong(yearValue));

				playerNameValueTextWithAgeGoldMedal
						.set(appendedAgeandPlayerNameValue.toString());

				context.write(keyYearValue, playerNameValueTextWithAgeGoldMedal);
			}

		}
	}
}
