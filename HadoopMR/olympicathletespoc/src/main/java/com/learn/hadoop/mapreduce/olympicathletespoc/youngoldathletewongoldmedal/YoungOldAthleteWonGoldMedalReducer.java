package com.learn.hadoop.mapreduce.olympicathletespoc.youngoldathletewongoldmedal;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YoungOldAthleteWonGoldMedalReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {

	private static final Log log = LogFactory
			.getLog(YoungOldAthleteWonGoldMedalReducer.class);

	private String WHITESPACE = "		";

	private String COMMA = ",";

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String youngAthleteName = null;

		String oldAthleteName = null;

		Integer minValue = Integer.MIN_VALUE;

		Integer maxValue = Integer.MAX_VALUE;

		Integer goldMedalValue = 0;

		String goldMedalTempValue = null;

		Integer minAgeValue = 100;

		Integer maxAgeValue = 0;

		Map<Integer, String> oldAthleteNameMap = new HashMap<Integer, String>();

		Map<Integer, String> youngAthleteNameMap = new HashMap<Integer, String>();

		log.debug("Executing Reduce Function");

		log.info("Executing Reduce Function");

		System.out.println("Executing Reduce Function");

		String playerNameValue = null;

		for (Text value : values) {

			if (!value.toString().isEmpty()) {

				String playerNameAndAgeValueString[] = value.toString().split(
						",");

				if (playerNameAndAgeValueString.length >= 2) {

					log.debug("Value Passed to Reduce" + value.toString());

					log.info("Logging Value Passed to Reduce "
							+ value.toString());

					playerNameValue = playerNameAndAgeValueString[0];

					goldMedalTempValue = playerNameAndAgeValueString[2];

					if (goldMedalTempValue.isEmpty()
							|| goldMedalTempValue.length() <= 0) {

						goldMedalValue = 0;
					} else {
						goldMedalValue = Integer.parseInt(goldMedalTempValue);
					}

					Integer ageValue = Integer
							.parseInt(playerNameAndAgeValueString[1]);

					if (ageValue >= minValue && ageValue >= maxAgeValue
							&& goldMedalValue > 0) {

						maxAgeValue = ageValue;

						oldAthleteName = playerNameValue;

						if (oldAthleteNameMap.containsKey(maxAgeValue)) {

							String oldPlayerNameValueBuffer = oldAthleteNameMap
									.get(maxAgeValue);

							oldPlayerNameValueBuffer = oldPlayerNameValueBuffer
									.concat(oldAthleteName).concat(COMMA);

							log.info("Adding of all old athlete Name:"
									+ oldPlayerNameValueBuffer);

							oldAthleteNameMap.put(ageValue,
									oldPlayerNameValueBuffer);
						} else {
							oldAthleteNameMap.put(maxAgeValue, playerNameValue);
						}

					}

					if (ageValue <= maxValue && ageValue <= minAgeValue
							&& goldMedalValue > 0) {

						minAgeValue = ageValue;

						youngAthleteName = playerNameValue;

						if (youngAthleteNameMap.containsKey(minAgeValue)) {

							String youngPlayerNameValueBuffer = youngAthleteNameMap
									.get(minAgeValue);

							youngPlayerNameValueBuffer = youngPlayerNameValueBuffer
									.concat(youngAthleteName).concat(COMMA);

							log.info("Adding of all Young athlete Name:"
									+ youngPlayerNameValueBuffer);

							youngAthleteNameMap.put(ageValue,
									youngPlayerNameValueBuffer);
						} else {
							youngAthleteNameMap.put(minAgeValue,
									playerNameValue);
						}
					}

				}
			}
		}

		String allYoungAthleteName = youngAthleteNameMap.get(minAgeValue);

		log.info("Logging of all Young athlete Name:" + allYoungAthleteName);

		String allOldAthleteName = oldAthleteNameMap.get(maxAgeValue);

		log.info("Logging of all Young athlete Name:" + allOldAthleteName);

		StringBuffer resultToWrite = new StringBuffer();

		resultToWrite.append("Young Athlete of the Year Who won Gold Medal:")
				.append(allYoungAthleteName).append(WHITESPACE)
				.append("Age of the Athlete:").append(minAgeValue)
				.append(WHITESPACE);

		resultToWrite.append("Oldest Athlete of the Year who won Gold Medal:")
				.append(allOldAthleteName).append(WHITESPACE)
				.append("Age of the Athlete:").append(WHITESPACE)
				.append(maxAgeValue);

		context.write(key, new Text(resultToWrite.toString()));

	}

}
