package com.learn.hadoop.mapreduce.olympicathletespoc.youngoldathletewongoldmedal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class YoungOldAthleteWonGoldMedalInvoker extends Configured implements
		Tool {

	private static final Log log = LogFactory
			.getLog(YoungOldAthleteWonGoldMedalInvoker.class);

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		int res = ToolRunner.run(new Configuration(),
				new YoungOldAthleteWonGoldMedalInvoker(), args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		log.debug("Invoking YoungOldAthleteWonGoldMedalInvoker");

		log.info("Invoking YoungOldAthleteWonGoldMedalInvoker");

		Job job = new Job();

		job.setJarByClass(YoungOldAthleteWonGoldMedalInvoker.class);

		job.setJobName("YoungOldAthleteWonGoldMedalInvoker");

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(YoungOldAthleteWonGoldMedalMapper.class);

		job.setReducerClass(YoungOldAthleteWonGoldMedalReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);

		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);

		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;
	}

}
