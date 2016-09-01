package de.oswoboda.querydata;

import java.util.Calendar;
import java.util.Date;

import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.AggregatorFactory;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.QueryMetric;
import org.kairosdb.client.builder.TimeUnit;
import org.kairosdb.client.response.Queries;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Results;

public class Main {
    public static void main(String[] args) throws Exception {
    	long startTime = System.currentTimeMillis();
    	System.out.println("Start: "+startTime);
    	HttpClient client = new HttpClient("http://"+args[0]+":25025");
    	
    	Calendar calendar = Calendar.getInstance();
    	calendar.set(Integer.parseInt(args[2]), 0, 1);
    	Date start = calendar.getTime();
    	calendar.set(Integer.parseInt(args[3]), 0, 1);
    	Date end = calendar.getTime();
    	
    	QueryBuilder builder = QueryBuilder.getInstance();
    	QueryMetric metric = builder.setStart(start)
    		.setEnd(end)
    		.addMetric(args[1]);
    	metric.addAggregator(AggregatorFactory.createMinAggregator(200, TimeUnit.YEARS));
    	QueryResponse qResponse = client.query(builder);
    	long queryTime = System.currentTimeMillis();
    	System.out.println("QueryTime: "+queryTime);
    	System.out.println("QueryDuration: "+(queryTime-startTime));
    	if (qResponse.getStatusCode() != 200) {
    		System.out.println(qResponse.getBody());
    	}
    	int sampleSize = 0;
    	System.out.println("Results:");
    	for (Queries queries : qResponse.getQueries()) {
    		sampleSize += queries.getSampleSize();
    		for (Results results : queries.getResults()) {
    			for (DataPoint dp : results.getDataPoints()) {
    				System.out.println(results.getName()+": "+dp.getValue());
    			}
    		}
    	}
    	System.out.println("\nsampleSize: "+sampleSize);
    	client.shutdown();
    	long endTime = System.currentTimeMillis();
    	System.out.println("Duration: "+(endTime-startTime));
    }
}
