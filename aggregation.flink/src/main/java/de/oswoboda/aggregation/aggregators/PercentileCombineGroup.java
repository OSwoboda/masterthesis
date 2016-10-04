package de.oswoboda.aggregation.aggregators;

import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class PercentileCombineGroup implements GroupCombineFunction<Tuple2<TreeMap<Long,AtomicInteger>,Integer>, Long> {
	
	private static final long serialVersionUID = 600554337355841710L;
	private int percentile;
	
	public PercentileCombineGroup(int percentile) {
		this.percentile = percentile;
	}
	
	@Override
	public void combine(Iterable<Tuple2<TreeMap<Long, AtomicInteger>, Integer>> in, Collector<Long> out) throws Exception {
		int count = 0;
		TreeMap<Long, AtomicInteger> histogram = null;
		for (Tuple2<TreeMap<Long, AtomicInteger>, Integer> tuple : in) {
			count += tuple.f1;
			if (histogram == null) {
				histogram = tuple.f0;
				continue;
			}
			for (Entry<Long, AtomicInteger> entry : tuple.f0.entrySet()) {
				AtomicInteger current;
				if ((current = histogram.putIfAbsent(entry.getKey(), entry.getValue())) != null) {
					current.addAndGet(entry.getValue().get());
				}
			}
		}
		int element = (int) Math.ceil(percentile/100d*count);
		int counter = 0;
		for (Entry<Long, AtomicInteger> entry : histogram.entrySet()) {
			counter += entry.getValue().get();
			if (counter >= element) {
				out.collect(entry.getKey());
				break;
			}
		}
	}
}
