package de.oswoboda.aggregation.aggregators;

public class Sum extends Aggregator {
	
	public Sum() {
		value = 0.;
	}
	
	@Override
	public void add(double update) {
		value += update;
	}

}
