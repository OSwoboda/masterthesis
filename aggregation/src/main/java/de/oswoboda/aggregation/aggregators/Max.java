package de.oswoboda.aggregation.aggregators;

public class Max extends Aggregator {
	
	private static final long serialVersionUID = 1L;

	public Max() {
		value = null;
	}
	
	@Override
	public void add(double update) {
		if (value == null || update > value) {
			value = update;
		}
	}
	
}
