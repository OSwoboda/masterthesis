package de.oswoboda.aggregation.aggregators;

import java.io.Serializable;

public abstract class Aggregator implements Serializable {

	private static final long serialVersionUID = 1L;
	
	protected int count;
	protected Long value;
	
	public abstract void add(long update);
	
	public double getResult() {
		return value;
	}
	
	public void merge(long update, int count) {
		add(update);
	}
	
	public int getCount() {
		return count;
	}
	
	public long getValue() {
		return value;
	}
	
	public static Class<? extends Aggregator> getAggregator(String aggregatorName) {
        switch (aggregatorName) {
            case "sum":
                return Sum.class;
            case "max":
                return Max.class;
            case "dev":
            case "min":
                return Min.class;
            case "avg":
                return Avg.class;
            case "count":
            default:
                return Min.class;
        }
    }
}
