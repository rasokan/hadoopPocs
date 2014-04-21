package com.learn.hadoop.mapreduce.olympicathletespoc.countgoldilverbronzemedalsbycountrybyeachyear;

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

public class CountGoldSilverBronzeMedalsByCountryByEachyYearInvoker extends
		Configured implements Tool {

	private static final Log log = LogFactory
			.getLog(CountGoldSilverBronzeMedalsByCountryByEachyYearInvoker.class);

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		log.debug("Invoking CountGoldSilverBronzeMedalsByCountryByEachyYearInvoker");

		log.info("Invoking CountGoldSilverBronzeMedalsByCountryByEachyYearInvoker");

		Job job = new Job();

		job.setJarByClass(CountGoldSilverBronzeMedalsByCountryByEachyYearInvoker.class);

		job.setJobName("CountGoldSilverBronzeMedalsByCountryByEachyYearInvoker");

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(CountGoldSilverBronzeMedalsByCountryByEachyYearMapper.class);

		job.setReducerClass(CountGoldSilverBronzeMedalsByCountryByEachyYearReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);

		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);

		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		int res = ToolRunner.run(new Configuration(),
				new CountGoldSilverBronzeMedalsByCountryByEachyYearInvoker(),
				args);
		System.exit(res);
	}

}
