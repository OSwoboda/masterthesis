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
import org.apache.log4j.Logger;

import de.oswoboda.aggregation.Metric;
import de.oswoboda.aggregation.aggregators.Aggregator;

public class AggregationIterator extends WrappingIterator
{
	private static final Logger log = Logger.getLogger(AggregationIterator.class);
	
	private Set<String> queryStations = new HashSet<>();
	private Aggregator aggregator;
	private long start;
	private long end;
	
	private Key last;
	
	@Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		System.out.println("init");
		log.trace("info: init");
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
		System.out.println("hasTop");
		while (super.hasTop()) {
			last = super.getTopKey();
			System.out.println(last.getRow());
			try {
				Metric metric = Metric.parse(super.getTopKey(), super.getTopValue());
				if (queryStations.isEmpty() || queryStations.contains(metric.getStation())) {
					if (metric.getTimestamp() >= start && metric.getTimestamp() <= end) {
						aggregator.add(metric.getValue());
					}
				}
				System.out.println(aggregator.getValue());
				super.next();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return last != null;
	}
	
	@Override
    public Key getTopKey() {
		System.out.println("getTopKey");
        return last;
    }
	
	@Override
    public Value getTopValue() {
		System.out.println("getTopValue");
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
		System.out.println("next");
        last = null;
    }
	
    public static Aggregator decodeValue(Value value) throws IOException, ClassNotFoundException {
    	System.out.println("decodeValue");
        ByteArrayInputStream bis = new ByteArrayInputStream(value.get());
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (Aggregator) ois.readObject();
    }
}
