package de.oswoboda.aggregation;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
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
		
		DataSource<Tuple2<Key, Value>> source = env.createHadoopInput(new AccumuloInputFormat(), Key.class, Value.class, job);
		source.print();

		// execute program
		env.execute("Accumulo Flink Aggregation");
	}
}
