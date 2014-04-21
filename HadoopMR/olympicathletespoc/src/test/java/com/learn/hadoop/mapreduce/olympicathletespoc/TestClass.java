package com.learn.hadoop.mapreduce.olympicathletespoc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestClass {

	private static final String COMMA = ",";

	private static final String WHITE_SPACE = "		";

	private static final String PERIOD = ".";

	@Test
	public void testList() throws IOException {

		File readFile = new File("/home/ashok/Desktop/olympicsSportbyCountry.csv");

		FileReader fread = new FileReader(readFile);

		BufferedReader bufferedReader = new BufferedReader(fread);

		List<String> fileReadList = new ArrayList<String>();

		String lineRead = null;

		while ((lineRead = bufferedReader.readLine()) != null) {

			lineRead = bufferedReader.readLine();

			fileReadList.add(lineRead);

			System.out.println(lineRead);

		}

		Map<String, List<Integer>> countryWithGSBMedalsCountMap = new java.util.HashMap<String, List<Integer>>();

		List<Integer> countryWithGSBMedalsListCount = null;

		Integer goldMedalValue = 0;

		Integer silverMedalValue = 0;

		Integer bronzeMedalValue = 0;

		for (String value : fileReadList) {

			String countryWithGSBMedals[] = value.toString().split(COMMA);

			String countryValue = countryWithGSBMedals[2];

			goldMedalValue = Integer.parseInt(countryWithGSBMedals[6]);

			silverMedalValue = Integer.parseInt(countryWithGSBMedals[7]);

			bronzeMedalValue = Integer.parseInt(countryWithGSBMedals[8]);

			if (countryWithGSBMedalsCountMap.containsKey(countryValue)) {

				countryWithGSBMedalsListCount = (ArrayList<Integer>) countryWithGSBMedalsCountMap
						.get(countryValue);

				for (Integer medalValue : countryWithGSBMedalsListCount) {

					System.out.println(medalValue);

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

				countryWithGSBMedalsListCount.set(0, updatedGoldCount);

				countryWithGSBMedalsListCount.set(1, updatedSilverCount);

				countryWithGSBMedalsListCount.set(2, updatedBronzeCount);

				countryWithGSBMedalsCountMap.put(countryValue,
						countryWithGSBMedalsListCount);

			} else {

				countryWithGSBMedalsListCount = new ArrayList<Integer>();

				countryWithGSBMedalsListCount.add(goldMedalValue);

				countryWithGSBMedalsListCount.add(silverMedalValue);

				countryWithGSBMedalsListCount.add(bronzeMedalValue);

				countryWithGSBMedalsCountMap.put(countryValue,
						countryWithGSBMedalsListCount);
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
		}

	}

}
