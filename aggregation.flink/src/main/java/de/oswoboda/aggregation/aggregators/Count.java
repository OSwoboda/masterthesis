package de.oswoboda.aggregation.aggregators;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

public class Count implements MapPartitionFunction<Tuple1<Long>, Long> {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unused")
	@Override
	public void mapPartition(Iterable<Tuple1<Long>> in, Collector<Long> out) throws Exception {
		long count = 0;
		for (Tuple1<Long> tuple : in) {
			++count;
		}
		out.collect(count);
	}

}
