package de.oswoboda.aggregation.aggregators;

import java.lang.invoke.MethodHandles;

public class Dev extends Aggregator {

	private static final long serialVersionUID = 1L;
	
	private long sum_x;

	public Dev() {
		// count of values
		count = 0;
		// sum of values^2
		value = 0L;
		// sum of values
		sum_x = 0L;
	}

	public long getSum_x() {
		return sum_x;
	}

	@Override
	public void add(long update) {
		++count;
		value += update*update;
		sum_x += update;
	}
	
	public void merge(Aggregator aggregator) {
		count += aggregator.getCount();
		value += aggregator.getValue();
		sum_x += ((Dev) aggregator).getSum_x();
	}
	
	@Override
	public double getResult() {
		return Math.sqrt((value-Math.pow(sum_x, 2)/count)/(count-1));
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
