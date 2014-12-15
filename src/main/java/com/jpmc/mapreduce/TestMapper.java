package com.jpmc.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.jpmc.parsers.BinaryObj;
import com.jpmc.parsers.VariableLengthBinaryRecordParser;

public class TestMapper extends
		Mapper<LongWritable, BytesWritable, LongWritable, BinaryObj> {
	@Override
	protected void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		if (value == null) {
			return;
		}
		BinaryObj obj = VariableLengthBinaryRecordParser.parser(value
				.getBytes());
		context.write(key, obj);
	}
}
