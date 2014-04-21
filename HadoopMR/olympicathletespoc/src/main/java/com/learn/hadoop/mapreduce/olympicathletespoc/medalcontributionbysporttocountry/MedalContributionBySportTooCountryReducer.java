package com.learn.hadoop.mapreduce.olympicathletespoc.medalcontributionbysporttocountry;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedalContributionBySportTooCountryReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {

	private static final String COMMA = ",";

	private static final String BLANK_SPACE = "\n";

	private static final Log log = LogFactory
			.getLog(MedalContributionBySportTooCountryReducer.class);

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		log.info("Executing Reduce function ");

		Map<String, Set<Map<String, Integer>>> medalContributionBySportTooCountMap = new HashMap<String, Set<Map<String, Integer>>>();

		Set<Map<String, Integer>> sportToMedalCountMapList = null;

		Map<String, Integer> sportToMedalCountMap = null;

		for (Text value : values) {

			String sportsNameAndCountryWithMedalValueString[] = value
					.toString().split(",");

			String countryNameValue = sportsNameAndCountryWithMedalValueString[0];

			String sportsNameValue = sportsNameAndCountryWithMedalValueString[1];

			Integer totalMedalCountValue = Integer
					.parseInt(sportsNameAndCountryWithMedalValueString[2]);

			if (medalContributionBySportTooCountMap
					.containsKey(countryNameValue)) {

				Boolean setFlag = false;

				sportToMedalCountMapList = medalContributionBySportTooCountMap
						.get(countryNameValue);

				Integer previousCountValue = 0;

				Integer updatedMedalCountValue = 0;

				for (Map<String, Integer> sportToMedalCountMapValue : sportToMedalCountMapList) {

					if (sportToMedalCountMapValue.containsKey(sportsNameValue)) {

						setFlag = true;

						previousCountValue = sportToMedalCountMapValue
								.get(sportsNameValue);

						updatedMedalCountValue = previousCountValue
								+ totalMedalCountValue;

						sportToMedalCountMapValue.put(sportsNameValue,
								updatedMedalCountValue);

						medalContributionBySportTooCountMap.put(
								countryNameValue, sportToMedalCountMapList);

						break;
					}
				}

				if (!setFlag) {

					sportToMedalCountMap = new HashMap<String, Integer>();

					sportToMedalCountMap.put(sportsNameValue,
							totalMedalCountValue);

					sportToMedalCountMapList.add(sportToMedalCountMap);

					medalContributionBySportTooCountMap.put(countryNameValue,
							sportToMedalCountMapList);

					setFlag = false;

				}
			}

			else {

				sportToMedalCountMap = new HashMap<String, Integer>();

				sportToMedalCountMap.put(sportsNameValue, totalMedalCountValue);

				sportToMedalCountMapList = new HashSet<Map<String, Integer>>();

				sportToMedalCountMapList.add(sportToMedalCountMap);

				medalContributionBySportTooCountMap.put(countryNameValue,
						sportToMedalCountMapList);
			}

		}

		StringBuffer resultToWrite = new StringBuffer();

		StringBuffer finalResultToWrite = new StringBuffer();

		for (Map.Entry<String, Set<Map<String, Integer>>> medalContributionBySportTooCountMapEntry : medalContributionBySportTooCountMap
				.entrySet()) {

			resultToWrite.replace(0, resultToWrite.length(), "");

			finalResultToWrite.replace(0, finalResultToWrite.length(), "");

			String countryValue = medalContributionBySportTooCountMapEntry
					.getKey();

			Set<Map<String, Integer>> medalCountMapList = medalContributionBySportTooCountMapEntry
					.getValue();

			for (Map<String, Integer> sportToMedalCount : medalCountMapList) {

				for (Map.Entry<String, Integer> sportToMedalCountMapEntry : sportToMedalCount
						.entrySet()) {

					String sportToMedalCountMapEntryKey = sportToMedalCountMapEntry
							.getKey();

					Integer sportToMedalCountMapEntryValue = sportToMedalCountMapEntry
							.getValue();

					resultToWrite.append(BLANK_SPACE).append("Sports Name: ")
							.append(sportToMedalCountMapEntryKey).append(COMMA)
							.append("Medal Count Value: ")
							.append(sportToMedalCountMapEntryValue);

				}

			}

			finalResultToWrite.append("Country : ").append(countryValue)
					.append(resultToWrite);

			context.write(key, new Text(finalResultToWrite.toString()));

		}
	}
}
