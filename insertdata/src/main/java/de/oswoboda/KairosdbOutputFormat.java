package de.oswoboda;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KairosdbOutputFormat implements OutputFormat<MetricBuilder> {

	private static final long serialVersionUID = 1L;
	private final Logger LOG = LoggerFactory.getLogger(this.getClass());
	private String masterIP;
	private HttpClient client;

	@Override
	public void close() throws IOException {
		client.shutdown();		
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		client = new HttpClient(masterIP);		
	}

	@Override
	public void writeRecord(MetricBuilder builder) throws IOException {
		try {
			Response response = client.pushMetrics(builder);
			if (response.getStatusCode() != 204) {
				for (String error : response.getErrors()) {
					LOG.error(error);
				}
			}
		} catch (URISyntaxException e) {
			LOG.error(e.getLocalizedMessage());
		}		
	}
	
	public String getMasterIP() {
		return masterIP;
	}
	public void setMasterIP(String masterIP) {
		this.masterIP = masterIP;
	}

}
