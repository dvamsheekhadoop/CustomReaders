package com.jpmc.inputformats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.jpmc.recordreaders.VariableLengthBinaryRecordReader;

public class VariableLengthFileInputFormat extends
		FileInputFormat<LongWritable, BytesWritable> {

	private static final double SPLIT_SLOP = 1.1; // 10% slop
	private long previousPosition = 0;

	@Override
	public RecordReader<LongWritable, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new VariableLengthBinaryRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);

		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);
		FileSystem fs = null;
		FSDataInputStream in = null;
		for (FileStatus file : files) {
			Path path = file.getPath();
			long length = file.getLen();
			fs = path.getFileSystem(job.getConfiguration());
			if (length != 0) {
				BlockLocation[] blkLocations;
				if (file instanceof LocatedFileStatus) {
					blkLocations = ((LocatedFileStatus) file)
							.getBlockLocations();
				} else {
					blkLocations = fs.getFileBlockLocations(file, 0, length);
				}
				if (isSplitable(job, path)) {
					long blockSize = file.getBlockSize();
					long splitSize = computeSplitSize(blockSize, minSize,
							maxSize);
					long bytesRemaining = length;
					in = fs.open(path);
					while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
						int blkIndex = getBlockIndex(blkLocations, length
								- bytesRemaining);
						long variableSplitSize = computeVariableSplitSize(
								splitSize, in);
						splits.add(makeSplit(path, length - bytesRemaining,
								variableSplitSize,
								blkLocations[blkIndex].getHosts(),
								blkLocations[blkIndex].getCachedHosts()));
						bytesRemaining -= variableSplitSize;
					}

					if (bytesRemaining != 0) {
						int blkIndex = getBlockIndex(blkLocations, length
								- bytesRemaining);
						splits.add(makeSplit(path, length - bytesRemaining,
								bytesRemaining,
								blkLocations[blkIndex].getHosts(),
								blkLocations[blkIndex].getCachedHosts()));
					}
				} else { // not splitable
					splits.add(makeSplit(path, 0, length,
							blkLocations[0].getHosts(),
							blkLocations[0].getCachedHosts()));
				}
			} else {
				// Create empty hosts array for zero length files
				splits.add(makeSplit(path, 0, length, new String[0]));
			}
		}
		// Save the number of input files for metrics/loadgen
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

		return splits;
	}

	protected long computeVariableSplitSize(long defaultSize,
			FSDataInputStream in) throws IOException {
		long currentSplitSize = 0;
		long pos = previousPosition;
		in.seek(previousPosition);
		while (in.available() > 0) {
			int len = in.readInt();
			pos += len;
			if (pos < defaultSize + previousPosition) {
				in.seek(len);
			} else {
				currentSplitSize = pos - previousPosition;
				break;
			}
		}
		return currentSplitSize;
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		final CompressionCodec codec = new CompressionCodecFactory(
				context.getConfiguration()).getCodec(file);
		return (null == codec);
	}

}
