package com.learn.hadoop.mapreduce.olympicathletespoc.playerparticipationofmorethanoneolympics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PlayerParticipationOfMorethanOneOlympicsReducer extends
		Reducer<Text, Text, Text, Text> {

	private static final Log log = LogFactory
			.getLog(PlayerParticipationOfMorethanOneOlympicsReducer.class);

	private static final String WHITE_SPACE = "		";

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		List<String> playerWithParticipatedYears = new ArrayList<String>();

		log.debug("Executing Reducer function");

		for (Text value : values) {

			String yearValue = value.toString();

			playerWithParticipatedYears.add(yearValue);

		}

		if (playerWithParticipatedYears.size() >= 2) {

			StringBuffer resultToWrite = new StringBuffer();

			resultToWrite.append("Player Participated in the year of:").append(
					WHITE_SPACE);

			for (String playerWithParticipatedYearsValue : playerWithParticipatedYears) {

				resultToWrite.append(playerWithParticipatedYearsValue).append(
						",");
			}

			resultToWrite.append(".");

			context.write(key, new Text(resultToWrite.toString()));
		}
	}
}
