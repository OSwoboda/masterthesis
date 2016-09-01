package de.oswoboda.querydata;

import java.util.Calendar;
import java.util.Date;

import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.AggregatorFactory;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.QueryMetric;
import org.kairosdb.client.builder.TimeUnit;
import org.kairosdb.client.response.GetResponse;
import org.kairosdb.client.response.Queries;
import org.kairosdb.client.response.QueryResponse;

public class Main {
    public static void main(String[] args) throws Exception {
    	long startTime = System.currentTimeMillis();
    	System.out.println("Start: "+startTime);
    	HttpClient client = new HttpClient("http://ring01.ext.mgm-tp.com:25025");
    	GetResponse stationResponse = client.getTagValues();
    	
    	Calendar calendar = Calendar.getInstance();
    	calendar.set(2005, 0, 1);
    	Date start = calendar.getTime();
    	calendar.set(2015, 0, 1);
    	Date end = calendar.getTime();
    	
    	QueryBuilder builder = QueryBuilder.getInstance();
    	QueryMetric metric = builder.setStart(start)
    		.setEnd(end)
    		.addMetric(args[0]);
    	int i = 0;
    	for (String station : stationResponse.getResults()) {
    		if (i <= 15000) {
    			metric.addTag("station", station);
    		}
    		i++;
    	}
    	metric.addAggregator(AggregatorFactory.createMinAggregator(15, TimeUnit.YEARS));
    	QueryResponse qResponse = client.query(builder);
    	long endTime = System.currentTimeMillis();
    	System.out.println("End: "+System.currentTimeMillis());
    	System.out.println("Duration: "+(endTime-startTime));
    	if (qResponse.getStatusCode() != 200) {
    		System.out.println(qResponse.getBody());
    	}
    	int sampleSize = 0;
    	for (Queries queries : qResponse.getQueries()) {
    		sampleSize += queries.getSampleSize();
    	}
    	System.out.println("sampleSize: "+sampleSize);
    	client.shutdown();
    }
}
