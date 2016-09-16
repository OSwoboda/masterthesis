package de.oswoboda.aggregation.aggregators;

public class Min extends Aggregator {
	
	private static final long serialVersionUID = 1L;

	public Min() {
		value = null;
		count = 0;
	}
	
	@Override
	public void add(double update) {
		++count;
		if (value == null || update < value) {
			value = update;
		}
	}
}
