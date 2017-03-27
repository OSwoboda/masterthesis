package de.oswoboda;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.hadoop.shaded.com.google.common.primitives.Longs;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;

public class Main {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool params = ParameterTool.fromArgs(args);
		
		String inputPath = params.get("input", "file:///root/masterthesis/0101.csv");
		Boolean kairosdb = params.getBoolean("kairosdb", false);
		Boolean accumulo = params.getBoolean("accumulo", false);

		DataSet<Tuple4<String, String, String, Long>> csvInput = env.readCsvFile(inputPath)
				.types(String.class, String.class, String.class, Long.class);
		
		if (kairosdb) {
			DataSet<MetricBuilder> builders = csvInput.groupBy(0,2).reduceGroup(new GroupReduceFunction<Tuple4<String,String,String,Long>, MetricBuilder>() {
		
				private static final long serialVersionUID = 1L;
	
				@Override
				public void reduce(Iterable<Tuple4<String, String, String, Long>> in, Collector<MetricBuilder> out) throws Exception {
					MetricBuilder builder = MetricBuilder.getInstance();
					Metric metric = null;
					for (Tuple4<String, String, String, Long> data : in) {
						if (metric == null) {
							metric = builder.addMetric(data.f2).addTag("station", data.f0);
						}
						DateFormat format = new SimpleDateFormat("yyyyMMdd");
						Date date = format.parse(data.f1);
						metric.addDataPoint(date.getTime(), data.f3);
					}
					out.collect(builder);
				}
				
			});
		
			KairosdbOutputFormat outputFormat = new KairosdbOutputFormat();
			outputFormat.setMasterIP(params.get("masterip", "http://localhost:25025"));
			
			builders.output(outputFormat);
		}
		
		if (accumulo) {			
			final String tableName = params.get("table", "oswoboda.bymonth");
			final boolean bymonth = (tableName.contains("month")) ? true : false;
			String instanceName = params.get("instance", "hdp-accumulo-instance");
			String zooServers = params.get("zoo", "sandbox:2181");
			String user = params.get("u", "root");
			String passwd = params.get("p", "P@ssw0rd");
			
			JobConf job = new JobConf(new Configuration());
			AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(passwd));
			ClientConfiguration clientConfig = ClientConfiguration.loadDefault();
			AccumuloOutputFormat.setZooKeeperInstance(job, clientConfig.withInstance(instanceName).withZkHosts(zooServers));
			
			BatchWriterConfig config = new BatchWriterConfig();
			config.setMaxMemory(10000000L);
			config.setMaxWriteThreads(32);
			
			AccumuloOutputFormat.setBatchWriterOptions(job, config);
			
			DataSet<Tuple2<Text, Mutation>> mutations = csvInput.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple4<String,String,String,Long>, Tuple2<Text, Mutation>>() {
				
				private static final long serialVersionUID = -6279402384188788056L;

				@Override
				public void reduce(Iterable<Tuple4<String, String, String, Long>> in, Collector<Tuple2<Text, Mutation>> out) throws Exception {
					Text table = new Text(tableName);
					Mutation mutation = null;
					String last = null;
					for (Tuple4<String, String, String, Long> data : in) {
						LocalDate date = LocalDate.parse(data.f1, DateTimeFormatter.BASIC_ISO_DATE);
						String current = date.format(bymonth ? TimeFormatUtils.YEAR_MONTH : TimeFormatUtils.YEAR);
						if (mutation == null || !last.equals(current)) {
							 if (mutation != null) {
								 out.collect(new Tuple2<Text, Mutation>(table, mutation));
							 }
							 last = current;
							 mutation = new Mutation(new Text(current+"_"+data.f0));
						}
						
						Text colFam = new Text(data.f2);
						Text colQual = new Text("");
						ColumnVisibility colVis = new ColumnVisibility("standard");
						long timestamp = Long.parseLong(date.format(TimeFormatUtils.DAY));
						if (!bymonth) {
							timestamp = date.getDayOfYear();
						}
						
						Value value = new Value(Longs.toByteArray(data.f3));						
						
						mutation.put(colFam, colQual, colVis, timestamp, value);
					}
					
					out.collect(new Tuple2<Text, Mutation>(table, mutation));
				}
				
			});
			
			HadoopOutputFormat<Text, Mutation> hadoopOutputFormat = new HadoopOutputFormat<Text, Mutation>(new AccumuloOutputFormat(), job);
			mutations.output(hadoopOutputFormat);
		}
		
		env.execute("Insert Data");
	}
}
