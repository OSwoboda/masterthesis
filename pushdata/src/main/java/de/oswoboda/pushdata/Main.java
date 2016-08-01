package de.oswoboda.pushdata;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.response.Response;

public class Main {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple4<String, String, String, Long>> csvInput = env.readCsvFile("file:///home/oswoboda/Dokumente/masterthesis/pushdata/1k.csv") //"hdfs:///user/oSwoboda/dataset/superghcnd_full_20160728.csv")
				.types(String.class, String.class, String.class, Long.class);
		
		csvInput.flatMap(new FlatMapFunction<Tuple4<String, String, String, Long>, Response>(){

			private static final long serialVersionUID = 725548890072477896L;

			@Override
			public void flatMap(Tuple4<String, String, String, Long> arg0, Collector<Response> arg1) throws Exception {
				DateFormat format = new SimpleDateFormat("yyyymmdd");
				Date date = format.parse(arg0.f1);
				MetricBuilder builder = MetricBuilder.getInstance();
				builder.addMetric(arg0.f2)
					.addTag("station", arg0.f0)
					.addDataPoint(date.getTime(), arg0.f3);
				HttpClient client = new HttpClient("http://localhost:8080");
				arg1.collect(client.pushMetrics(builder));
				client.shutdown();
			}
			
		});
	}

}
