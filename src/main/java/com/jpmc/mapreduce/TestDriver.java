package com.jpmc.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.jpmc.inputformats.VariableLengthFileInputFormat;
import com.jpmc.parsers.BinaryObj;

public class TestDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "Case Count");
		job.setJarByClass(TestDriver.class);

		job.setInputFormatClass(VariableLengthFileInputFormat.class);

		job.setMapperClass(TestMapper.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(BinaryObj.class);

		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		if (job.isSuccessful()) {
			return 0;
		} else {
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TestDriver(), args);
		System.exit(exitCode);
	}

}
