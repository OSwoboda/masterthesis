package de.oswoboda;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;

public class Job {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool params = ParameterTool.fromArgs(args);
		
		String inputPath = params.get("input", "hdfs:///user/oSwoboda/dataset/0101.csv");

		DataSet<Tuple4<String, String, String, Long>> csvInput = env.readCsvFile(inputPath)
				.types(String.class, String.class, String.class, Long.class);
		
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

		env.execute("Insert Data");
	}
}
