package com.learn.hadoop.mapreduce.olympicathletespoc.countplayersonsportforeachyear;

import java.util.logging.Logger;

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

import com.learn.hadoop.mapreduce.olympicathletespoc.countplayerofeachyear.CountPlayerOfEachYearInvoker;

public class CountPlayersOnSportForEachYearInvoker extends Configured implements
		Tool {

	private static final Logger log = Logger
			.getLogger(CountPlayersOnSportForEachYearInvoker.class.getName());

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		log.info("Executing Client Driver");

		Job job = new Job();

		job.setJarByClass(CountPlayersOnSportForEachYearInvoker.class);

		job.setJobName("CountPlayersOnSportForEachYearInvoker");

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(CountPlayersOnSportForEachYearMapper.class);

		job.setReducerClass(CountPlayersOnSportForEachYearReducer.class);

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
				new CountPlayersOnSportForEachYearInvoker(), args);
		System.exit(res);

	}

}
