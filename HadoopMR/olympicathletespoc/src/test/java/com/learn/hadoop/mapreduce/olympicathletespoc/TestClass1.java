package com.learn.hadoop.mapreduce.olympicathletespoc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestClass1 {

	private static final String COMMA = ",";

	private static final String BLANK_SPACE = "\n";

	/**
	 * @throws IOException
	 */
	@Test
	public void testSize() throws IOException {

		File readFile = new File(
				"/home/ashok/Desktop/olympicsSportbyCountryupdated.csv");

		FileReader fread = new FileReader(readFile);

		BufferedReader bufferedReader = new BufferedReader(fread);

		List<String> fileReadList = new ArrayList<String>();

		String lineRead = null;

		while ((lineRead = bufferedReader.readLine()) != null) {

			lineRead = bufferedReader.readLine();

			fileReadList.add(lineRead);

			System.out.println(lineRead);

		}

		Map<String, Set<Map<String, Integer>>> medalContributionBySportTooCountMap = new HashMap<String, Set<Map<String, Integer>>>();

		Set<Map<String, Integer>> sportToMedalCountMapList = null;

		Map<String, Integer> sportToMedalCountMap = null;

		for (String value : fileReadList) {

			String sportsNameAndCountryWithMedalValueString[] = value
					.toString().split(",");

			String countryNameValue = sportsNameAndCountryWithMedalValueString[2];

			String sportsNameValue = sportsNameAndCountryWithMedalValueString[5];

			Integer totalMedalCountValue = Integer
					.parseInt(sportsNameAndCountryWithMedalValueString[6]);

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

			System.out.println(finalResultToWrite.toString());

		}

	}
}
