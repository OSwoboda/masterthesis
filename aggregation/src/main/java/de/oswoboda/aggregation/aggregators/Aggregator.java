package de.oswoboda.aggregation.aggregators;

import java.io.Serializable;

public abstract class Aggregator implements Serializable {

	private static final long serialVersionUID = 1L;
	
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
