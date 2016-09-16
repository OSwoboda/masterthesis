package de.oswoboda.aggregation.aggregators;

public class Avg extends Aggregator {
	
	private static final long serialVersionUID = 1L;

	public Avg() {
		count = 0;
		value = 0.;
	}
	
	@Override
	public void add(double update) {
		value += update;
		++count;
	}
	
	@Override
	public void merge(double update, int count) {
		value += update;
		this.count += count;
	}
	
	@Override
	public Double getResult() {
		return value/count;
	}

}
