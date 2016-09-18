package de.oswoboda.aggregation.aggregators;

public class Sum extends Aggregator {
	
	private static final long serialVersionUID = 1L;

	public Sum() {
		value = 0L;
	}
	
	@Override
	public void add(long update) {
		value += update;
	}

}
