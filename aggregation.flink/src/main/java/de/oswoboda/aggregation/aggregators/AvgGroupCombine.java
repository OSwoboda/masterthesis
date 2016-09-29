package de.oswoboda.aggregation.aggregators;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;

public class AvgGroupCombine implements GroupCombineFunction<Tuple, Double> {
	
	private static final long serialVersionUID = 1L;

	@Override
	public void combine(Iterable<Tuple> in, Collector<Double> out) throws Exception {
		Long sum = 0L;
		Double count = 0d;
		for (Tuple tuple : in) {
			sum += (long)tuple.getField(0);
			count += (double)tuple.getField(1);
		}
		out.collect(sum/count);
	}
}
