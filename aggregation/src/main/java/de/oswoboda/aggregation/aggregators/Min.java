package de.oswoboda.aggregation.aggregators;

public class Min extends Aggregator {
	
	public Min() {
		value = null;
	}
	
	@Override
	public void add(double update) {
		if (value == null || update < value) {
			value = update;
		}
	}
}
