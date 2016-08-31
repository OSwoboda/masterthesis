package de.oswoboda.querydata;

import java.util.Calendar;
import java.util.Date;

import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.QueryMetric;
import org.kairosdb.client.response.GetResponse;
import org.kairosdb.client.response.QueryResponse;

public class Main {
    public static void main(String[] args) throws Exception {
    	long startTime = System.currentTimeMillis();
    	System.out.println("Start: "+startTime);
    	HttpClient client = new HttpClient("http://ring01.ext.mgm-tp.com:25025");
    	GetResponse stationResponse = client.getTagValues();
    	
    	Calendar calendar = Calendar.getInstance();
    	calendar.set(2015, 0, 1);
    	Date start = calendar.getTime();
    	calendar.set(2015, 0, 2);
    	Date end = calendar.getTime();
    	
    	QueryBuilder builder = QueryBuilder.getInstance();
    	QueryMetric metric = builder.setStart(start)
    		.setEnd(end)
    		.addMetric("TMIN");
    	for (String station : stationResponse.getResults()) {
    		metric.addTag("station", station);
    	}
    	QueryResponse qResponse = client.query(builder);
    	System.out.println(qResponse.getBody());
    	client.shutdown();
    	long endTime = System.currentTimeMillis();
    	System.out.println("End: "+System.currentTimeMillis());
    	System.out.println("Duration: "+(endTime-startTime));
    }
}