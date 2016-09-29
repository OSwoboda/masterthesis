package de.oswoboda.aggregation.aggregators;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class AvgGroupCombine implements GroupCombineFunction<Tuple3<Long, Integer, Long>, Double> {
	
	private static final long serialVersionUID = 1L;

	@Override
	public void combine(Iterable<Tuple3<Long, Integer, Long>> in, Collector<Double> out) throws Exception {
		Long sum = 0L;
		Double count = 0d;
		for (Tuple3<Long, Integer, Long> tuple : in) {
			sum += tuple.f0;
			count += tuple.f1;
		}
		out.collect(sum/count);
	}
}
