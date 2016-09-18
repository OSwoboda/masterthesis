package de.oswoboda.aggregation.aggregators;

public class Avg extends Aggregator {
	
	private static final long serialVersionUID = 1L;

	public Avg() {
		count = 0;
		value = 0L;
	}
	
	@Override
	public void add(long update) {
		value += update;
		++count;
	}
	
	@Override
	public void merge(long update, int count) {
		value += update;
		this.count += count;
	}
	
	@Override
	public double getResult() {
		return ((double) value)/count;
	}

}
