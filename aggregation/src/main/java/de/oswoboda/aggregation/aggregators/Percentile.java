package de.oswoboda.aggregation.aggregators;

import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Percentile extends Aggregator {

	private static final long serialVersionUID = 1L;

	private TreeMap<Long, AtomicInteger> histogram = new TreeMap<>();
	private int percentile;
	
	public Percentile() {
		count = 0;
	}
	
	public void setPercentile(int percentile) {
		this.percentile = percentile;
	}
	
	@Override
	public void add(long update) {
		++count;
		AtomicInteger current;
		if ((current = histogram.putIfAbsent(update, new AtomicInteger(1))) != null) {
			current.incrementAndGet();
		}
	}
	
	@Override
	public void merge(Aggregator aggregator) {
		count += aggregator.getCount();
		for (Entry<Long, AtomicInteger> entry : ((Percentile)aggregator).getHistogram().entrySet()) {
			AtomicInteger current;
			if ((current = histogram.putIfAbsent(entry.getKey(), entry.getValue())) != null) {
				current.addAndGet(entry.getValue().get());
			}
		}
	}
	
	@Override
	public Double getResult() {
		int element = (int) Math.ceil(percentile/100d*count);
		int counter = 0;
		for (Entry<Long, AtomicInteger> entry : histogram.entrySet()) {
			counter += entry.getValue().get();
			if (counter >= element) {
				return (double)entry.getKey();
			}
		}
		return null;
	}
	
	public TreeMap<Long, AtomicInteger> getHistogram() {
		return histogram;
	}
}
