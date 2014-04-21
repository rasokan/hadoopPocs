package com.learn.hadoop.mapreduce.olympicathletespoc.countgoldilverbronzemedalsbycountrybyeachyear;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountGoldSilverBronzeMedalsByCountryByEachyYearReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {

	private static final Log log = LogFactory
			.getLog(CountGoldSilverBronzeMedalsByCountryByEachyYearReducer.class);

	private static final String COMMA = ",";

	private static final String WHITE_SPACE = "		";

	private static final String PERIOD = ".";

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, List<Integer>> countryWithGSBMedalsCountMap = new java.util.HashMap<String, List<Integer>>();

		List<Integer> countryWithGSBMedalsListCount = null;

		log.debug("Executing Reduce function");

		Integer goldMedalValue = 0;

		Integer silverMedalValue = 0;

		Integer bronzeMedalValue = 0;

		for (Text value : values) {

			String countryWithGSBMedals[] = value.toString().split(COMMA);

			log.info("Read Value:" + value.toString());

			String countryValue = countryWithGSBMedals[0];

			goldMedalValue = Integer.parseInt(countryWithGSBMedals[1]);

			silverMedalValue = Integer.parseInt(countryWithGSBMedals[2]);

			bronzeMedalValue = Integer.parseInt(countryWithGSBMedals[3]);

			log.info("Read Gold Value:" + goldMedalValue);
			log.info("Read Silver Value:" + silverMedalValue);
			log.info("Read Bronze Value:" + bronzeMedalValue);

			if (countryWithGSBMedalsCountMap.containsKey(countryValue)) {

				countryWithGSBMedalsListCount = (ArrayList<Integer>) countryWithGSBMedalsCountMap
						.get(countryValue);

				for (Integer medalValue : countryWithGSBMedalsListCount) {

					log.info("Looping through Mapped Value" + medalValue);
				}

				Integer previousGoldCount = countryWithGSBMedalsListCount
						.get(0);

				Integer previousSilverCount = countryWithGSBMedalsListCount
						.get(1);

				Integer previousBronzeCount = countryWithGSBMedalsListCount
						.get(2);

				Integer updatedGoldCount = previousGoldCount + goldMedalValue;

				Integer updatedSilverCount = previousSilverCount
						+ silverMedalValue;

				Integer updatedBronzeCount = previousBronzeCount
						+ bronzeMedalValue;

				log.info("Updated Gold Value:" + updatedGoldCount);

				log.info("Updated Silver Value:" + updatedSilverCount);

				log.info("Updated Bronze Value:" + updatedBronzeCount);

				countryWithGSBMedalsListCount.set(0, updatedGoldCount);

				countryWithGSBMedalsListCount.set(1, updatedSilverCount);

				countryWithGSBMedalsListCount.set(2, updatedBronzeCount);

				log.info("Size of list:" + countryWithGSBMedalsListCount.size());

				countryWithGSBMedalsCountMap.put(countryValue,
						countryWithGSBMedalsListCount);

			} else {

				countryWithGSBMedalsListCount = new ArrayList<Integer>();

				log.info("Entered into Bronze Value Initlialization:"
						+ bronzeMedalValue);

				log.info("Entered into Gold Value Initlialization:"
						+ goldMedalValue);

				log.info("Entered into silver Value Initlialization:"
						+ silverMedalValue);

				countryWithGSBMedalsListCount.add(goldMedalValue);

				countryWithGSBMedalsListCount.add(silverMedalValue);

				countryWithGSBMedalsListCount.add(bronzeMedalValue);

				countryWithGSBMedalsCountMap.put(countryValue,
						countryWithGSBMedalsListCount);

				log.info("Size of list:" + countryWithGSBMedalsListCount.size());
			}
		}

		for (Map.Entry<String, List<Integer>> countryWithGSBMedalsCountMapEntry : countryWithGSBMedalsCountMap
				.entrySet()) {

			StringBuffer resultToWrite = new StringBuffer();

			String countryValue = countryWithGSBMedalsCountMapEntry.getKey();

			List<Integer> countryWithGSBMedalsCountMapList = countryWithGSBMedalsCountMapEntry
					.getValue();

			resultToWrite
					.append("Country:")
					.append(countryValue)
					.append(WHITE_SPACE)
					.append("Gold Medal Count of the year:"
							+ countryWithGSBMedalsCountMapList.get(0))
					.append(COMMA)
					.append("Silver Medal" + "count of the year:"
							+ countryWithGSBMedalsCountMapList.get(1))
					.append(COMMA)
					.append("Bronze Medal Count of the yerar:"
							+ countryWithGSBMedalsCountMapList.get(2))
					.append(PERIOD);

			context.write(key, new Text(resultToWrite.toString()));
		}
	}
}
