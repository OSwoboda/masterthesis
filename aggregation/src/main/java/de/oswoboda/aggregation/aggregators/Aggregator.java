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
	
	public void merge(Aggregator aggregator) {
		add(aggregator.getValue());
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
            	return Dev.class;
            case "min":
                return Min.class;
            case "avg":
                return Avg.class;
            case "count":
            	return Count.class;
            default:
                return Min.class;
        }
    }
}
