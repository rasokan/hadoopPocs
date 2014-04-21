package com.learn.hadoop.mapreduce.olympicathletespoc.playerparticipationofmorethanoneolympics;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PlayerParticipationOfMorethanOneOlympicsMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private static final Log log = LogFactory
			.getLog(PlayerParticipationOfMorethanOneOlympicsMapper.class);

	private static final Text playerNameValueKey = new Text();

	private static final Text participatedYearValueValue = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		log.debug("Executing Mapper function");

		String lineRead = value.toString();

		String lineReadValues[] = lineRead.split(",");

		String playerNameValue = lineReadValues[0];

		String participatedYearValue = lineReadValues[3];

		playerNameValueKey.set(playerNameValue);

		participatedYearValueValue.set(participatedYearValue);

		context.write(playerNameValueKey, participatedYearValueValue);
	}
}
