package de.oswoboda.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import de.oswoboda.aggregation.aggregators.Aggregator;
import de.oswoboda.aggregation.aggregators.Dev;
import de.oswoboda.aggregation.iterators.AggregationIterator;

public class Main {
	
	public static void main(String[] args) throws Exception {
		
		Options options = new Options();
		options.addOption("metricName", false, "name of the metric, e.g. TMIN");
		options.addOption("tableName", false, "name of the table, e.g. oswoboda.bymonth");
		options.addOption("start", false, "start date, e.g. 20100101");
		options.addOption("end", false, "end date, e.g. 20150101");
		options.addOption("agg", false, "which aggregation should be used, e.g. min");
		options.addOption(Option.builder()
				.longOpt("station")
				.hasArgs()
				.argName("stations")
				.valueSeparator(',')
				.build());
		options.addOption("instance", false, "accumulo instance name");
		options.addOption(Option.builder()
				.longOpt("zoo")
				.hasArgs()
				.argName("zooServers")
				.valueSeparator(',')
				.build());
		options.addOption("u", "user", false, "accumulo user");
		options.addOption("p", "passwd", false, "accumulo user password");
		
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
		String metricName = cmd.getOptionValue("metricName", "TMIN");
		String tableName = cmd.getOptionValue("tableName", "oswoboda.bymonth");
		String start = cmd.getOptionValue("start", "20100101");
		String end = cmd.getOptionValue("end", "20150101");
		boolean bymonth = tableName.contains("bymonth") ? true : false;
		String aggregation = cmd.getOptionValue("agg", "min");
		
		TreeSet<String> stations = new TreeSet<>();
		if (cmd.hasOption("stations")) {
			stations.addAll(Arrays.asList(cmd.getOptionValues("stations")));
		}
		
		Date startDate = TimeFormatUtils.parse(start, TimeFormatUtils.YEAR_MONTH_DAY);
		Date endDate = TimeFormatUtils.parse(end, TimeFormatUtils.YEAR_MONTH_DAY);
		String instanceName = cmd.getOptionValue("instance", "hdp-accumulo-instance");
		String zooServers = cmd.hasOption("zoo") ?  String.join(",", cmd.getOptionValues("zoo")) : "localhost:2181";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);

		Connector conn = inst.getConnector(cmd.getOptionValue("user"), new PasswordToken(cmd.getOptionValue("passwd")));
		
		Authorizations auths = new Authorizations("standard");
		BatchScanner bscan = conn.createBatchScanner(tableName, auths, 10);
		try {
			Set<Range> ranges = Collections.singleton(new Range());
			if (!stations.isEmpty()) {
				if (bymonth) {
					ranges = Collections.singleton(new Range(
							TimeFormatUtils.YEAR_MONTH.format(startDate)+"_"+stations.first(),
							TimeFormatUtils.YEAR_MONTH.format(endDate)+"_"+stations.last()));
				} else {
					ranges = Collections.singleton(new Range(
							TimeFormatUtils.YEAR.format(startDate)+"_"+stations.first(),
							TimeFormatUtils.YEAR.format(endDate)+"_"+stations.last()));
				}
			}
			bscan.setRanges(ranges);
			bscan.fetchColumn(new Text("data_points"), new Text(metricName));
			
			IteratorSetting is = new IteratorSetting(500, AggregationIterator.class);
			is.addOption("stations", StringUtils.join(stations, ","));
			is.addOption("start", String.valueOf(startDate.getTime()));
			is.addOption("end", String.valueOf(endDate.getTime()));
			Class<? extends Aggregator> aggClass = Aggregator.getAggregator(aggregation);
			is.addOption("aggregation", aggClass.getName());
			
			bscan.addScanIterator(is);
	
			List<Aggregator> resultAggregators = new ArrayList<>();
			for(Entry<Key,Value> entry : bscan) {
				Aggregator resultAggregator = AggregationIterator.decodeValue(entry.getValue());
				System.out.println(resultAggregator.getCount());
				System.out.println(resultAggregator.getValue());
				if (resultAggregator instanceof Dev) {
					System.out.println(((Dev)resultAggregator).getSum_x());
				}
				System.out.println(resultAggregator.getResult());
			    resultAggregators.add(resultAggregator);
			}
			Aggregator aggregator = aggClass.newInstance();
			for (Aggregator resultAggregator : resultAggregators) {
				aggregator.merge(resultAggregator);
			}
			System.out.println(aggregator.getResult());
		} finally {
			bscan.close();
		}
	}

}
