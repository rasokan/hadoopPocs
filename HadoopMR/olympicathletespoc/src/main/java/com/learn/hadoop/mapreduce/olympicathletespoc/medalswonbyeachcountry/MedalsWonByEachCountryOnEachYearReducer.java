package com.learn.hadoop.mapreduce.olympicathletespoc.medalswonbyeachcountry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedalsWonByEachCountryOnEachYearReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {

	private static final Log log = LogFactory
			.getLog(MedalsWonByEachCountryOnEachYearReducer.class);

	private static final String COMMA = ",";

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Integer sum = 0;

		Map<String, Integer> countrywithPlayerMedalCountMap = new HashMap<String, Integer>();

		for (Text value : values) {

			String countryValueAndMedalOfPlayerValue[] = value.toString()
					.split(COMMA);

			String countryValue = countryValueAndMedalOfPlayerValue[0];

			Integer medalCountOfPlayerValue = Integer
					.parseInt(countryValueAndMedalOfPlayerValue[1]);

			if (countrywithPlayerMedalCountMap.containsKey(countryValue)) {

				Integer previousMedalCountOfPlayerValue = countrywithPlayerMedalCountMap
						.get(countryValue);

				Integer updatedMedalCountOfPlayerValue = previousMedalCountOfPlayerValue
						+ medalCountOfPlayerValue;

				countrywithPlayerMedalCountMap.put(countryValue,
						updatedMedalCountOfPlayerValue);

			} else {

				countrywithPlayerMedalCountMap.put(countryValue,
						medalCountOfPlayerValue);
			}
		}

		for (Map.Entry<String, Integer> countrywithPlayerMedalCountMapEntry : countrywithPlayerMedalCountMap
				.entrySet()) {

			String countryValueKey = countrywithPlayerMedalCountMapEntry
					.getKey();

			Integer medalCountValue = countrywithPlayerMedalCountMapEntry
					.getValue();

			StringBuffer resultToWrite = new StringBuffer();

			resultToWrite.append("Country:").append(countryValueKey)
					.append("Medal Count for the year:")
					.append(medalCountValue);

			resultToWrite.append("\n");

			context.write(key, new Text(resultToWrite.toString()));
		}

	}
}
