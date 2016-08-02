package de.oswoboda;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.response.Response;

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.java file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
public class Job {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		final ParameterTool params = ParameterTool.fromArgs(args);
		String inputPath = params.get("input", "hdfs:///user/oSwoboda/dataset/superghcnd_full_20160728.csv");
		String outputPath = params.get("output", "hdfs:///user/oSwoboda/output/insertdata");

		DataSet<Tuple4<String, String, String, Long>> csvInput = env.readCsvFile(inputPath)
				.types(String.class, String.class, String.class, Long.class);
		
		DataSet<String> responses = csvInput.flatMap(new FlatMapFunction<Tuple4<String, String, String, Long>, String>(){

			private static final long serialVersionUID = 725548890072477896L;

			@Override
			public void flatMap(Tuple4<String, String, String, Long> arg0, Collector<String> arg1) throws Exception {
				DateFormat format = new SimpleDateFormat("yyyymmdd");
				Date date = format.parse(arg0.f1);
				MetricBuilder builder = MetricBuilder.getInstance();
				builder.addMetric(arg0.f2)
					.addTag("station", arg0.f0)
					.addDataPoint(date.getTime(), arg0.f3);
				HttpClient client = new HttpClient("http://localhost:25025");
				Response response = client.pushMetrics(builder);
				if (response.getStatusCode() != 204) {
					arg1.collect(arg0.f0+","+arg0.f1+","+arg0.f2+","+arg0.f3+","+response.getStatusCode());
				}
				client.shutdown();
			}
			
		}).setParallelism(32);
		
		responses.writeAsText(outputPath, WriteMode.OVERWRITE);

		// execute program
		env.execute("Insert Data");
	}
}
