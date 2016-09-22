package de.oswoboda.aggregation;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.mapreduce.Job;

public class Main {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		//final ParameterTool params = ParameterTool.fromArgs(args);
		
		Job job = Job.getInstance();
		AccumuloInputFormat.setInputTableName(job, "oswoboda.bymonth");
		AccumuloInputFormat.setConnectorInfo(job, "root", new PasswordToken("P@ssw0rd"));
		AccumuloInputFormat.setScanAuthorizations(job, new Authorizations("standard"));
		ClientConfiguration clientConfig = new ClientConfiguration();
		AccumuloInputFormat.setZooKeeperInstance(job, clientConfig.withInstance("hdp-accumulo-instance").withZkHosts("localhost:2181"));
		
		DataSet<Tuple2<Key, Value>> source = env.createHadoopInput(new AccumuloInputFormat(), Key.class, Value.class, job);
		DataSet<Long> result = source.flatMap(new FlatMapFunction<Tuple2<Key,Value>, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(Tuple2<Key, Value> in, Collector<Long> out) throws Exception {
				Metric metric = Metric.parse(in.f0, in.f1);
				if (metric.getStation().equals("GME00102292")) {
					out.collect(metric.getValue());
				}
			}
		}).aggregate(Aggregations.MIN, 0);
		
		result.print();

		// execute program
		env.execute("Accumulo Flink Aggregation");
	}
}
