package com.learn.hadoop.mapreduce.olympicathletespoc.playersofeachyear;

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

public class PlayerOfEachYearInvoker extends Configured implements Tool {

	private static final Logger log = Logger
			.getLogger(PlayerOfEachYearInvoker.class.getName());

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		log.info("Invoking Client Driver");
		
		int res = ToolRunner.run(new Configuration(),
				new PlayerOfEachYearInvoker(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		log.info("Executing Client Driver");
		
		Job job = new Job();

		job.setJarByClass(PlayerOfEachYearInvoker.class);

		job.setJobName("PlayerOfEachYearInvoker");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(PlayerOfEachYearMapper.class);

		job.setReducerClass(PlayerOfEachYearReducer.class);

		job.setOutputKeyClass(LongWritable.class);

		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;
	}
}
