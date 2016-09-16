package de.oswoboda.aggregators;

public class Max extends Aggregator {
	
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
