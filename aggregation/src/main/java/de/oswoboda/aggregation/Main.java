package de.oswoboda.aggregation;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.oswoboda.aggregation.aggregators.Aggregator;
import de.oswoboda.aggregation.iterators.AggregationIterator;

public class Main {
	
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
	
	public static void main(String[] args) throws Exception {		
		
		Options options = new Options();
		options.addOption("metricName", true, "name of the metric, e.g. TMIN");
		options.addOption("tableName", true, "name of the table, e.g. oswoboda.bymonth");
		options.addOption("start", true, "start date, e.g. 20100101");
		options.addOption("end", true, "end date, e.g. 20150101");
		options.addOption("agg", true, "which aggregation should be used, e.g. min");
		options.addOption("percentile" , true, "which percentile should be calculated, e.g. 50 for median");
		options.addOption(Option.builder()
				.longOpt("stations")
				.hasArgs()
				.argName("station")
				.valueSeparator(',')
				.build());
		options.addOption("instance", true, "accumulo instance name");
		options.addOption(Option.builder()
				.longOpt("zoo")
				.hasArgs()
				.argName("zooServer")
				.valueSeparator(',')
				.build());
		options.addOption("u", "user", true, "accumulo user");
		options.addOption("p", "passwd", true, "accumulo user password");
		options.addOption("baseline", false, "if only a baseline should be made");
		
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
		String metricName = cmd.getOptionValue("metricName", "TMIN");
		String tableName = cmd.getOptionValue("tableName", "oswoboda.bymonth");
		String start = cmd.getOptionValue("start", "20100101");
		String end = cmd.getOptionValue("end", "20150101");
		boolean bymonth = tableName.contains("bymonth") ? true : false;
		String aggregation = cmd.getOptionValue("agg", "min");
		String percentile = cmd.getOptionValue("percentile", "50");
		
		TreeSet<String> stations = new TreeSet<>();
		if (cmd.hasOption("stations")) {
			stations.addAll(Arrays.asList(cmd.getOptionValues("stations")));
		}
		
		LocalDate startDate = LocalDate.parse(start, DateTimeFormatter.BASIC_ISO_DATE);
		LocalDate endDate = LocalDate.parse(end, DateTimeFormatter.BASIC_ISO_DATE);
		String instanceName = cmd.getOptionValue("instance", "hdp-accumulo-instance");
		String zooServers = cmd.hasOption("zoo") ?  String.join(",", cmd.getOptionValues("zoo")) : "localhost:2181";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);

		Connector conn = inst.getConnector(cmd.getOptionValue("user", "root"), new PasswordToken(cmd.getOptionValue("passwd", "P@ssw0rd")));
		
		Authorizations auths = new Authorizations("standard");
		BatchScanner bscan = conn.createBatchScanner(tableName, auths, 320);
		long startMillis;
		if (cmd.hasOption("baseline")) {
			try {
				bscan.setRanges(Collections.singleton(new Range()));
				startMillis = System.currentTimeMillis();
				LOG.info("batchScan started");
				long results = 0;
				for(@SuppressWarnings("unused") Entry<Key,Value> entry : bscan) {
					++results;
				}
				LOG.info("Number of results: "+results);				
			} finally {
				bscan.close();
			}
			long endMillis = System.currentTimeMillis();
			LOG.info("batchScan finished; Duration: "+(endMillis-startMillis)+"ms");
			System.exit(0);
		}
		
		try {
			LocalDate endRowDate = endDate;
			if (stations.isEmpty()) {
				endRowDate = bymonth ? endDate.plusMonths(1) : endDate.plusYears(1);
			}
			String startRow = (bymonth) ? startDate.format(TimeFormatUtils.YEAR_MONTH) : startDate.format(TimeFormatUtils.YEAR);
			String endRow = (bymonth) ? endRowDate.format(TimeFormatUtils.YEAR_MONTH) : endRowDate.format(TimeFormatUtils.YEAR);
			Set<Range> ranges = (stations.isEmpty()) ? 
					Collections.singleton(new Range(startRow, endRow)) :
						Collections.singleton(new Range(startRow+"_"+stations.first(), endRow+"_"+stations.last()));
			bscan.setRanges(ranges);
			bscan.fetchColumnFamily(new Text(metricName));
			IteratorSetting is = new IteratorSetting(500, AggregationIterator.class);
			is.addOption("stations", StringUtils.join(stations, ","));
			is.addOption("start", String.valueOf(startDate.toEpochDay()));
			is.addOption("end", String.valueOf(endDate.toEpochDay()));
			Class<? extends Aggregator> aggClass = Aggregator.getAggregator(aggregation);
			is.addOption("aggregation", aggClass.getName());
			if (aggregation.equals("percentile")) {
				is.addOption("percentile", percentile);
			}
			
			bscan.addScanIterator(is);
			Aggregator aggregator = null;
			int results = 0;
			
			startMillis = System.currentTimeMillis();
			LOG.info("batchScan started");
			
			for(Entry<Key,Value> entry : bscan) {
				++results;
				Aggregator resultAggregator = AggregationIterator.decodeValue(entry.getValue());
				if (aggregator == null) {
					aggregator = resultAggregator;
				}
			    aggregator.merge(resultAggregator);
			    /*if (resultAggregator.getValue() != null) {
			    	LOG.info("Results from Aggregator "+results+": Value "+resultAggregator.getValue()+"; Count: "+resultAggregator.getCount());
			    }*/
			}
			
			LOG.info("Number of results: "+results);
			
			if (aggregator.getResult() != null) {
				LOG.info("EndResult: "+aggregator.getResult());
			} else {
				LOG.info("No Result!");
			}
		} finally {
			bscan.close();
		}
		long endMillis = System.currentTimeMillis();
		LOG.info("batchScan finished; Duration: "+(endMillis-startMillis)+"ms");
	}

}
