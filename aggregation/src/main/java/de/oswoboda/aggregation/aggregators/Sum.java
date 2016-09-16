package de.oswoboda.aggregation.aggregators;

public class Sum extends Aggregator {
	
	private static final long serialVersionUID = 1L;

	public Sum() {
		value = 0.;
	}
	
	@Override
	public void add(double update) {
		value += update;
	}

}
