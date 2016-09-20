package de.oswoboda.aggregation.aggregators;

import java.lang.invoke.MethodHandles;

public class Avg extends Aggregator {
	
	private static final long serialVersionUID = 1L;

	public Avg() {
		count = 0;
		value = 0L;
	}
	
	@Override
	public void add(long update) {
		value += update;
		++count;
	}
	
	@Override
	public void merge(Aggregator aggregator) {
		count += aggregator.getCount();
		value += aggregator.getValue();
	}
	
	@Override
	public double getResult() {
		return ((double) value)/count;
	}
	
	public static void main(String[] args) throws InstantiationException, IllegalAccessException {
		Aggregator agg = (Aggregator) MethodHandles.lookup().lookupClass().newInstance();
		for (int i = 1; i <= 5; i++) {
			agg.add(i);
		}
		Aggregator aggOne = (Aggregator) MethodHandles.lookup().lookupClass().newInstance();
		for (int i = 1; i <= 3; i++) {
			aggOne.add(i);
		}
		Aggregator aggTwo = (Aggregator) MethodHandles.lookup().lookupClass().newInstance();
		for (int i = 4; i <= 5; i++) {
			aggTwo.add(i);
		}
		aggOne.merge(aggTwo);
		System.out.println(agg.getResult() == aggOne.getResult());
	}

}
