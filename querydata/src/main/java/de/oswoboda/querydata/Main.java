package de.oswoboda.querydata;

import java.util.Calendar;
import java.util.Date;

import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.QueryMetric;
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
    	calendar.set(2015, 0, 1);
    	Date start = calendar.getTime();
    	calendar.set(2015, 0, 2);
    	Date end = calendar.getTime();
    	
    	QueryBuilder builder = QueryBuilder.getInstance();
    	QueryMetric metric = builder.setStart(start)
    		.setEnd(end)
    		.addMetric("TMIN");
    	int i = 0;
    	for (String station : stationResponse.getResults()) {
    		if (i <= Integer.parseInt(args[0])) {
    			metric.addTag("station", station);
    		}
    		i++;
    	}
    	System.out.println("Stations :"+i);
    	System.out.println(builder.toString());
    	QueryResponse qResponse = client.query(builder);
    	int sampleSize = 0;
    	for (Queries queries : qResponse.getQueries()) {
    		sampleSize += queries.getSampleSize();
    	}
    	System.out.println("sampleSize: "+sampleSize);
    	client.shutdown();
    	long endTime = System.currentTimeMillis();
    	System.out.println("End: "+System.currentTimeMillis());
    	System.out.println("Duration: "+(endTime-startTime));
    }
}
