package de.oswoboda.aggregation.aggregators;

import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Percentile extends Aggregator {
	//private static final Logger LOG = LoggerFactory.getLogger(Percentile.class);
	private static final long serialVersionUID = 1L;

	private TreeMap<Long, AtomicInteger> histogram = new TreeMap<>();
	private int percentile;
	
	public Percentile(int percentile) {
		this.percentile = percentile;
		count = 0;
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
				//LOG.info("Percentile: "+percentile+"; Count: "+count+"; Counter: "+counter+"; Element: "+element+"; Value: "+entry.getKey());
				return (double)entry.getKey();
			}
		}
		return null;
	}
	
	public TreeMap<Long, AtomicInteger> getHistogram() {
		return histogram;
	}
	
	public static void main(String[] args) throws InstantiationException, IllegalAccessException {
		Aggregator agg = new Percentile(50);
		for (int i = 1; i <= 5; i++) {
			agg.add(i);
		}
		Aggregator aggOne = new Percentile(50);
		for (int i = 1; i <= 3; i++) {
			aggOne.add(i);
		}
		Aggregator aggTwo = new Percentile(50);
		for (int i = 4; i <= 5; i++) {
			aggTwo.add(i);
		}
		aggOne.merge(aggTwo);
		System.out.println(agg.getResult().equals(aggOne.getResult()));
	}
}
