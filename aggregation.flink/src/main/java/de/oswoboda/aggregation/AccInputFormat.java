package de.oswoboda.aggregation;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CleanUp;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;

public class AccInputFormat extends AccumuloInputFormat {

	// protected static final Logger log =
	// LoggerFactory.getLogger(AccInputFormat.class);

	@Override
	public RecordReader<Key, Value> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		log.setLevel(getLogLevel(context));

		// Override the log level from the configuration as if the InputSplit
		// has one it's the more correct one to use.
		if (split instanceof org.apache.accumulo.core.client.mapreduce.RangeInputSplit) {
			org.apache.accumulo.core.client.mapreduce.RangeInputSplit accSplit = (org.apache.accumulo.core.client.mapreduce.RangeInputSplit) split;
			Level level = accSplit.getLogLevel();
			if (null != level) {
				log.setLevel(level);
			}
		} else {
			throw new IllegalArgumentException("No RecordReader for " + split.getClass().toString());
		}

		return new RecordReaderBase<Key, Value>() {
			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				if (scannerIterator.hasNext()) {
					++numKeysRead;
					Entry<Key, Value> entry = scannerIterator.next();
					currentK = currentKey = entry.getKey();
					currentV = entry.getValue();
					if (log.isTraceEnabled())
						log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
					return true;
				}
				return false;
			}
			
			@Override
			public void close() {
				super.close();
				CleanUp.shutdownNow();
			}
		};
	}
}
