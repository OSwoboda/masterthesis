package de.oswoboda.aggregators;

public abstract class Aggregator {
	protected int count;
	protected Double value;
	
	public abstract void add(double update);
	
	public Double getResult() {
		return value;
	}
	
	public void merge(double update, int count) {
		add(update);
	}
	
	public int getCount() {
		return count;
	}
	
	public Double getValue() {
		return value;
	}
}
