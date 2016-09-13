package de.oswoboda.iterators;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

public class AggregationIterator extends WrappingIterator
{
	private Set<String> queryMetrics = new HashSet<>();
	private Set<String> queryStations = new HashSet<>();
	
	@Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        queryMetrics.addAll(Arrays.asList(options.get("metrics").split(",")));
        queryStations.addAll(Arrays.asList(options.get("stations").split(",")));
	}
	
	@Override
    public boolean hasTop() {
		while (super.hasTop()) {
			try {
				Metric metric = Metric.parse(super.getTopKey(), super.getTopValue());
			} catch (ParseException e) {
				e.printStackTrace();
			}			
		}
		return true;
	}
}
