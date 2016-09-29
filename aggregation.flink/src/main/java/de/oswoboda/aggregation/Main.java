package de.oswoboda.aggregation;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class Main {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool params = ParameterTool.fromArgs(args);
		
		final LocalDate startDate = LocalDate.parse(params.get("start", "20140101"), DateTimeFormatter.BASIC_ISO_DATE);
		final LocalDate endDate = LocalDate.parse(params.get("end", "20150101"), DateTimeFormatter.BASIC_ISO_DATE);
		
		String tableName = params.get("tableName", "oswoboda.bymonth");
		boolean bymonth = tableName.contains("month") ? true : false;
		TreeSet<String> stations = new TreeSet<>();
		if (params.has("stations")) {
			stations.addAll(Arrays.asList(params.get("stations")));
		}
		
		LocalDate endRowDate = endDate;
		if (stations.isEmpty()) {
			endRowDate = bymonth ? endDate.plusMonths(1) : endDate.plusYears(1);
		}
		String startRow = (bymonth) ? startDate.format(TimeFormatUtils.YEAR_MONTH) : startDate.format(TimeFormatUtils.YEAR);
		String endRow = (bymonth) ? endRowDate.format(TimeFormatUtils.YEAR_MONTH) : endRowDate.format(TimeFormatUtils.YEAR);
		Set<Range> ranges = (stations.isEmpty()) ? 
				Collections.singleton(new Range(startRow, endRow)) :
					Collections.singleton(new Range(startRow+"_"+stations.first(), endRow+"_"+stations.last()));
		
		Job job = Job.getInstance();
		AccumuloInputFormat.fetchColumns(job, Collections.singleton(new Pair<Text, Text>(new Text("data_points"), new Text(params.get("metricName", "TMIN")))));
		AccumuloInputFormat.setBatchScan(job, true);
		AccumuloInputFormat.setRanges(job, ranges);
		AccumuloInputFormat.setInputTableName(job, tableName);
		AccumuloInputFormat.setConnectorInfo(job, "root", new PasswordToken(params.get("passwd", "P@ssw0rd")));
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations("standard"));
		ClientConfiguration clientConfig = new ClientConfiguration();
		AccumuloInputFormat.setZooKeeperInstance(job, clientConfig.withInstance("hdp-accumulo-instance").withZkHosts(params.get("zoo", "localhost:2181")));
		
		DataSet<Tuple2<Key,Value>> source = env.createHadoopInput(new AccumuloInputFormat(), Key.class, Value.class, job);
		source = source.filter(new FilterFunction<Tuple2<Key,Value>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(Tuple2<Key, Value> in) throws Exception {
					
				long start = startDate.toEpochDay();
				long end = endDate.toEpochDay();
				long timestamp = Metric.parseTimestamp(in.f0);
				if (timestamp >= start && timestamp <= end) {
					
					Set<String> stations = new HashSet<>();
					if (params.has("stations")) {
						stations.addAll(Arrays.asList(params.get("stations")));
					}
					
					if (stations.isEmpty() || stations.contains(Metric.parseStation(in.f0))) {
						return true;
					}
				}
				return false;
			}
		});
		DataSet<Tuple2<Long, Integer>> data = source.flatMap(new FlatMapFunction<Tuple2<Key,Value>, Tuple2<Long, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(Tuple2<Key, Value> in, Collector<Tuple2<Long, Integer>> out) throws Exception {
				out.collect(new Tuple2<Long, Integer>(Metric.parseValue(in.f1), 1));
			}
		});
		switch (params.get("agg", "min")) {
		case "avg":	data.sum(0).andSum(1).combineGroup(new GroupCombineFunction<Tuple2<Long, Integer>, Double>() {

						private static final long serialVersionUID = 1L;

						@Override
						public void combine(Iterable<Tuple2<Long, Integer>> in, Collector<Double> out) throws Exception {
							Long sum = 0L;
							Double count = 0d;
							for (Tuple2<Long, Integer> tuple : in) {
								sum += tuple.f0;
								count += tuple.f1;
							}
							out.collect(sum/count);
						}
					}).print();
					break;
		case "count":	data.sum(1).print();
						break;
		case "max":	data.max(0).print();
					break;
		case "sum":	data.sum(0).print();
					break;
		case "min":	
		default:	data.min(0).print();
					break;
		}
	}
}
