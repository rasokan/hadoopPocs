package com.learn.hadoop.mapreduce.olympicathletespoc.countriesnomedalcountoneachyear;

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

import com.learn.hadoop.mapreduce.olympicathletespoc.youngestandoldestathleteofeachyear.YoungestAndOldestAthleteOfEachYear;

public class CountriesNoMedalCountOnEachYearInvoker extends Configured
		implements Tool {

	private static final Log log = LogFactory
			.getLog(CountriesNoMedalCountOnEachYearInvoker.class);

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		int res = ToolRunner.run(new Configuration(),
				new CountriesNoMedalCountOnEachYearInvoker(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		log.debug("Invoking CountriesNoMedalCountOnEachYearInvoker");

		log.info("Invoking CountriesNoMedalCountOnEachYearInvoker");

		Job job = new Job();

		job.setJarByClass(CountriesNoMedalCountOnEachYearInvoker.class);

		job.setJobName("CountriesNoMedalCountOnEachYearInvoker");

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(CountriesNoMedalCountOnEachYearMapper.class);

		job.setReducerClass(CountriesNoMedalCountOnEachYearReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);

		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);

		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;
	}

}
