package de.oswoboda.aggregation;

import java.io.IOException;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

public class AccInputFormat extends AccumuloInputFormat {
	
	protected static final Logger log = Logger.getLogger(AccInputFormat.class);
	
	private RecordReader<Key, Value> recordReader = null;
	
	@Override
	public RecordReader<Key, Value> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		log.error("createRecordReader");
		recordReader = super.createRecordReader(split, context);
		return recordReader;
	}
	
	public RecordReader<Key, Value> getRecordReader() {
		return recordReader;
	}
}
