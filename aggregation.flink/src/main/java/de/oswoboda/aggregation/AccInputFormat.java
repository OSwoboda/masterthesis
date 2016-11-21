package de.oswoboda.aggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccInputFormat extends AccumuloInputFormat {
	
	protected static final Logger log = LoggerFactory.getLogger(AccInputFormat.class);
	
	private List<RecordReader<Key, Value>> recordReaders = new ArrayList<>();
	
	@Override
	public RecordReader<Key, Value> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		RecordReader<Key, Value> recordReader = super.createRecordReader(split, context);
		recordReaders.add(recordReader);
		return recordReader;
	}
	
	public List<RecordReader<Key, Value>> getRecordReaders() {
		return recordReaders;
	}
}
