package de.oswoboda.aggregation.aggregators;

import java.io.Serializable;

public abstract class Aggregator implements Serializable {

	private static final long serialVersionUID = 1L;
	
	protected int count;
	protected Long value;
	
	public abstract void add(long update);
	
	public Double getResult() {
		return (double)value;
	}
	
	public void merge(Aggregator aggregator) {
		if (aggregator.getValue() != null) {
			add(aggregator.getValue());
		}
	}
	
	public int getCount() {
		return count;
	}
	
	public Long getValue() {
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
            case "percentile":
            	return Percentile.class;
            default:
                return Min.class;
        }
    }
}
