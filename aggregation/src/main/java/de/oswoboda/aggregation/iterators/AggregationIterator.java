package de.oswoboda.aggregation.iterators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

import de.oswoboda.aggregation.Metric;
import de.oswoboda.aggregation.TimeFormatUtils;
import de.oswoboda.aggregation.aggregators.Aggregator;

public class AggregationIterator extends WrappingIterator
{		
	private TreeSet<String> queryStations = new TreeSet<>();
	private Aggregator aggregator;
	private long start;
	private long end;
	
	private Key last;
	private Metric lastMetric = null;
	
	@Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        String stations = options.get("stations");
        if (stations.length() > 0) {
        	queryStations.addAll(Arrays.asList(stations.split(",")));
        }
        start = Long.parseLong(options.get("start"));
        end = Long.parseLong(options.get("end"));
        String aggregation = options.get("aggregation");
        try {
			aggregator = (Aggregator) Class.forName(aggregation).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
    public boolean hasTop() {
		while (super.hasTop()) {
			last = super.getTopKey();
			try {
				Metric metric = Metric.parse(last, super.getTopValue());
				if (lastMetric != null) {
					if (!metric.getStation().equals(lastMetric.getStation()) && lastMetric.getStation().equals(queryStations.last())) {
						Range range;
						Calendar calendar = Calendar.getInstance();
						calendar.setTimeInMillis(lastMetric.getTimestamp());
						if (lastMetric.isMonthFormat()) {
							calendar.add(Calendar.MONTH, 1);
							range = new Range(TimeFormatUtils.YEAR_MONTH.format(calendar.getTime())+"_"+queryStations.first(),
									TimeFormatUtils.YEAR_MONTH.format(end)+"_"+queryStations.last());
						} else {
							calendar.add(Calendar.YEAR, 1);
							range = new Range(TimeFormatUtils.YEAR.format(calendar.getTime())+"_"+queryStations.first());
						}
						super.seek(range, Collections.singleton(last.getColumnFamilyData()), true);
						lastMetric = metric;
						continue;
					}
				}
				lastMetric = metric;
				if (queryStations.isEmpty() || queryStations.contains(lastMetric.getStation())) {
					if (lastMetric.getTimestamp() >= start && lastMetric.getTimestamp() <= end) {
						aggregator.add(lastMetric.getValue());
					}					
				}				
				super.next();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return last != null;
	}
	
	@Override
    public Key getTopKey() {
        return last;
    }
	
	@Override
    public Value getTopValue() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(aggregator);
            out.flush();
            return new Value(bos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
	
	@Override
    public void next() throws IOException {
        last = null;
    }
	
    public static Aggregator decodeValue(Value value) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(value.get());
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (Aggregator) ois.readObject();
    }
}