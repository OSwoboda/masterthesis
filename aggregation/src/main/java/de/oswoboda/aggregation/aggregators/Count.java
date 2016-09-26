package de.oswoboda.aggregation.aggregators;

import java.lang.invoke.MethodHandles;

public class Count extends Aggregator {
	
	private static final long serialVersionUID = 1L;

	public Count() {
		count = 0;
	}

	@Override
	public void add(long update) {
		++count;
	}
	
	@Override
	public void merge(Aggregator aggregator) {
		count += aggregator.getCount();
	}
	
	@Override
	public Double getResult() {
		return Double.valueOf(count);
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
