package com.learn.hadoop.mapreduce.olympicathletespoc.medalcontributionbysporttocountry;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedalContributionBySportTooCountryMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	private static final Log log = LogFactory
			.getLog(MedalContributionBySportTooCountryMapper.class);

	private LongWritable yearValueKey = new LongWritable();

	private Text countryNameAndSportsNameValueText = new Text();

	private static final String COMMA = ",";

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		log.info("Executing Map Function");

		String lineRead = value.toString();

		log.info("Read line in  Map Function" + lineRead);

		String lineReadValues[] = lineRead.split(",");

		String yearValueString = lineReadValues[3];

		String sportsNameValue = lineReadValues[5];

		String countryNameValue = lineReadValues[2];

		String totalMedalCountValue = lineReadValues[9];

		if ((!yearValueString.isEmpty()) && (!sportsNameValue.isEmpty())
				&& (!countryNameValue.isEmpty())
				&& (!totalMedalCountValue.isEmpty())) {

			Long yearValue = Long.parseLong(lineReadValues[3]);

			StringBuffer appendCountryNameAndSportsNameValue = new StringBuffer();

			appendCountryNameAndSportsNameValue.append(countryNameValue)
					.append(COMMA).append(sportsNameValue).append(COMMA)
					.append(totalMedalCountValue);

			countryNameAndSportsNameValueText
					.set(appendCountryNameAndSportsNameValue.toString());

			context.write(new LongWritable(yearValue),
					countryNameAndSportsNameValueText);
		}

	}

}
