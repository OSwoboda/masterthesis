package de.oswoboda.iterators;

import java.text.ParseException;
import java.util.Calendar;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class Metric {
	
	private String station;
	private String metricName;
	private long timestamp;
	private double value;
	private boolean isMonthFormat = true;
	
	public Metric(String metricName, long timestamp, String station, double value, boolean isMonthFormat) {
		this.metricName = metricName;
		this.timestamp = timestamp;
		this.station = station;
		this.value = value;
		this.isMonthFormat = isMonthFormat;
	}
	
	public static Metric parse(Key key, Value value) throws ParseException {
		String rowKey = key.getRow().toString();
		String[] split = rowKey.split("_");
		boolean isMonthFormat = (split[0].length() == 6) ? true : false;
		String station = split[1];
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(TimeFormatUtils.parse(split[0], (isMonthFormat) ? TimeFormatUtils.YEAR_MONTH : TimeFormatUtils.YEAR));
		calendar.add((isMonthFormat) ? Calendar.DAY_OF_MONTH : Calendar.DAY_OF_YEAR, (int) key.getTimestamp());
		long timestamp = calendar.getTimeInMillis();
		double doubleValue = Double.parseDouble(value.toString());
		String metricName = key.getColumnQualifier().toString();
		
		return new Metric(metricName, timestamp, station, doubleValue, isMonthFormat);
	}

	public String getStation() {
		return station;
	}

	public void setStation(String station) {
		this.station = station;
	}

	public String getMetricName() {
		return metricName;
	}

	public void setMetricName(String metricName) {
		this.metricName = metricName;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public boolean isMonthFormat() {
		return isMonthFormat;
	}

	public void setMonthFormat(boolean isMonthFormat) {
		this.isMonthFormat = isMonthFormat;
	}
	
}
