package de.oswoboda.iterators;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class Metric {
	
	private Set<String> stations = new HashSet<>();
	private String metricName;
	private Date timestamp;
	private double value;
	private boolean isMonthFormat = true;
	
	public Metric(String metricName, Date timestamp, Set<String> stations, double value, boolean isMonthFormat) {
		this.metricName = metricName;
		this.timestamp = timestamp;
		this.stations = stations;
		this.value = value;
		this.isMonthFormat = isMonthFormat;
	}
	
	public static Metric parse(Key key, Value value) throws ParseException {
		String rowKey = key.getRow().toString();
		String[] split = rowKey.split("_");
		boolean isMonthFormat = (split[0].length() == 6) ? true : false;
		Set<String> stations = new HashSet<>();
		stations.add(split[1]);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(TimeFormatUtils.parse(split[0], (isMonthFormat) ? TimeFormatUtils.YEAR_MONTH : TimeFormatUtils.YEAR));
		calendar.add((isMonthFormat) ? Calendar.DAY_OF_MONTH : Calendar.DAY_OF_YEAR, (int) key.getTimestamp());
		Date timestamp = calendar.getTime();
		double doubleValue = Double.parseDouble(value.toString());
		String metricName = key.getColumnQualifier().toString();
		
		return new Metric(metricName, timestamp, stations, doubleValue, isMonthFormat);
	}

	public Set<String> getStations() {
		return stations;
	}

	public void setStations(Set<String> stations) {
		this.stations = stations;
	}
	
	public void addStation(String station) {
		stations.add(station);
	}

	public String getMetricName() {
		return metricName;
	}

	public void setMetricName(String metricName) {
		this.metricName = metricName;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
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
