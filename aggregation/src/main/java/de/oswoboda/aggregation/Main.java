package de.oswoboda.aggregation;

import java.util.ArrayList;
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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import de.oswoboda.aggregation.aggregators.Aggregator;
import de.oswoboda.aggregation.iterators.AggregationIterator;

public class Main {
	
	public static void main(String[] args) throws Exception {
		String metricName = "TMIN";
		String tableName = "oswoboda.bymonth";
		String start = "20140101";
		String end = "20150101";
		boolean bymonth = true;
		String aggregation = "min";
		
		TreeSet<String> stations = new TreeSet<>();
		stations.add("GME00102292"); // Leipzig-Schkeuditz
		
		Date startDate = TimeFormatUtils.parse(start, TimeFormatUtils.YEAR_MONTH_DAY);
		Date endDate = TimeFormatUtils.parse(end, TimeFormatUtils.YEAR_MONTH_DAY);
		String instanceName = "hdp-accumulo-instance";
		String zooServers = "sandbox:2181";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);

		Connector conn = inst.getConnector("root", new PasswordToken("P@ssw0rd"));
		
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
				System.out.println("Bla");
			    resultAggregators.add(AggregationIterator.decodeValue(entry.getValue()));
			}
			Aggregator aggregator = aggClass.newInstance();
			for (Aggregator resultAggregator : resultAggregators) {
				aggregator.merge(resultAggregator.getValue(), resultAggregator.getCount());
			}
			System.out.println(aggregator.getResult());
		} finally {
			bscan.close();
		}
	}

}
