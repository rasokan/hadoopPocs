package com.learn.hadoop.mapreduce.olympicathletespoc.countplayersonsportforeachyear;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountPlayersOnSportForEachYearReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {

	private static final Log log = LogFactory
			.getLog(CountPlayersOnSportForEachYearReducer.class);

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, Integer> playerSportsNameValueMap = new HashMap<String, Integer>();

		Integer playerCount = 0;

		log.info("Executing reduce function");

		for (Text value : values) {

			String playerNameWithSportsValue[] = value.toString().split(",");

			String sportsNameValue = playerNameWithSportsValue[1];

			if (playerSportsNameValueMap.containsKey(sportsNameValue)) {

				Integer preViousPlayerCount = playerSportsNameValueMap
						.get(sportsNameValue);

				Integer updatedPlayerCount = preViousPlayerCount + 1;

				playerSportsNameValueMap.put(sportsNameValue,
						updatedPlayerCount);

			} else {

				playerSportsNameValueMap.put(sportsNameValue, playerCount + 1);
			}

		}

		for (Map.Entry<String, Integer> playerSportsNameValueMapEntry : playerSportsNameValueMap
				.entrySet()) {

			String sportsValueKey = playerSportsNameValueMapEntry.getKey();

			Integer playerCountValue = playerSportsNameValueMapEntry.getValue();

			StringBuffer resultToWrite = new StringBuffer();

			resultToWrite
					.append("Sports:")
					.append(sportsValueKey)
					.append("Player Count for the Particular sport on that Year:")
					.append(playerCountValue);

			resultToWrite.append("\n");

			context.write(key, new Text(resultToWrite.toString()));
		}

	}
}
