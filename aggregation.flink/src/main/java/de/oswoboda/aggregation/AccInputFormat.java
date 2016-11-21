package de.oswoboda.aggregation;

import java.io.IOException;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class AccInputFormat extends AccumuloInputFormat {
	
	private RecordReader<Key, Value> recordReader;
	
	@Override
	public RecordReader<Key, Value> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		System.out.println("1");
		recordReader = super.createRecordReader(split, context);
		return recordReader;
	}
	
	public RecordReader<Key, Value> getRecordReader() {
		return recordReader;
	}
}
