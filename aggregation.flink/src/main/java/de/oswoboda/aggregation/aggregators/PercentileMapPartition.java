package de.oswoboda.aggregation.aggregators;

import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.util.CleanUp;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class PercentileMapPartition implements MapPartitionFunction<Tuple3<Long,Integer,Long>, Tuple2<TreeMap<Long, AtomicInteger>, Integer>> {
	
	private static final long serialVersionUID = -8210222191926235961L;

	@Override
	public void mapPartition(Iterable<Tuple3<Long, Integer, Long>> in, Collector<Tuple2<TreeMap<Long, AtomicInteger>, Integer>> out) throws Exception {
		int count = 0;
		TreeMap<Long, AtomicInteger> histogram = new TreeMap<>();
		for (Tuple3<Long, Integer, Long> tuple : in) {
			++count;
			AtomicInteger current;
			if ((current = histogram.putIfAbsent(tuple.f0, new AtomicInteger(1))) != null) {
				current.incrementAndGet();
			}
		}
		out.collect(new Tuple2<TreeMap<Long, AtomicInteger>, Integer>(histogram, count));
		CleanUp.shutdownNow();
	}
}
