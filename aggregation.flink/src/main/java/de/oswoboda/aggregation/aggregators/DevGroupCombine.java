package de.oswoboda.aggregation.aggregators;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class DevGroupCombine implements GroupCombineFunction<Tuple3<Long, Integer, Long>, Double> {

	private static final long serialVersionUID = 1L;

	@Override
	public void combine(Iterable<Tuple3<Long, Integer, Long>> in, Collector<Double> out) throws Exception {
		Long sum_x = 0L;
		Integer count = 0;
		Long sum_x2 = 0L;
		for (Tuple3<Long, Integer, Long> tuple : in) {
			sum_x += tuple.f0;
			count += tuple.f1;
			sum_x2 += tuple.f2;
		}
		out.collect(Math.sqrt((sum_x2-Math.pow(sum_x, 2)/count)/(count-1)));
	}

}
