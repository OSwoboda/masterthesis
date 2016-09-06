package de.oswoboda;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

public class AccumuloOutputFormat implements OutputFormat<Mutation> {

	private static final long serialVersionUID = 1L;
	private Connector conn;
	private BatchWriter writer;

	@Override
	public void close() throws IOException {
		try {
			writer.close();
		} catch (MutationsRejectedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void configure(Configuration arg0) {
		Instance inst = new ZooKeeperInstance("hdp-accumulo-instance", "sandbox.hortonworks.com:2181");
		try {
			conn = inst.getConnector("accumulo", new PasswordToken("P@ssw0rd"));
		} catch (AccumuloException | AccumuloSecurityException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(10000000L); // bytes available to batchwriter for buffering mutations
		try {
			writer = conn.createBatchWriter("month", config);
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}		
	}

	@Override
	public void writeRecord(Mutation mutation) throws IOException {
		try {
			writer.addMutation(mutation);
		} catch (MutationsRejectedException e) {
			e.printStackTrace();
		}
	}

}
