package de.oswoboda.aggregation.iterators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.oswoboda.aggregation.Metric;
import de.oswoboda.aggregation.aggregators.Aggregator;

public class AggregationIterator extends WrappingIterator
{	
	private static final Logger LOG = LoggerFactory.getLogger(AggregationIterator.class);
	
	private Set<String> queryStations = new HashSet<>();
	private Aggregator aggregator;
	private long start;
	private long end;
	
	private Key last;
	
	@Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        queryStations.addAll(Arrays.asList(options.get("stations").split(",")));
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
				Metric metric = Metric.parse(super.getTopKey(), super.getTopValue());
				LOG.error("queryStations empty: "+queryStations.isEmpty());
				if (queryStations.isEmpty() || queryStations.contains(metric.getStation())) {
					if (metric.getTimestamp() >= start && metric.getTimestamp() <= end) {
						LOG.error("addValue: "+metric.getValue());
						aggregator.add(metric.getValue());
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