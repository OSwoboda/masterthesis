package de.oswoboda.aggregation.aggregators;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

public class Sum implements MapPartitionFunction<Tuple1<Long>, Long> {

	private static final long serialVersionUID = 1L;

	@Override
	public void mapPartition(Iterable<Tuple1<Long>> in, Collector<Long> out) throws Exception {
		Long sum = 0L;
		for (Tuple1<Long> tuple : in) {
			sum += tuple.f0;
		}
		out.collect(sum);
	}

}
