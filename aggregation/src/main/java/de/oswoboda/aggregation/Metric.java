package de.oswoboda.aggregation;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.time.Year;
import java.time.YearMonth;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class Metric {
	
	private String station;
	private String metricName;
	private long timestamp;
	private long value;
	private boolean isMonthFormat = true;
	
	public Metric(String metricName, long timestamp, String station, long value, boolean isMonthFormat) {
		this.metricName = metricName;
		this.timestamp = timestamp;
		this.station = station;
		this.value = value;
		this.isMonthFormat = isMonthFormat;
	}
	
	public static Metric parse(Key key, Value value) throws ParseException {
		String station = parseStation(key);
		String metricName = parseMetricName(key);
		long timestamp = parseTimestamp(key);
		long longValue = parseValue(value);
		boolean isMonthFormat = parseMonthFormat(key);
		
		return new Metric(metricName, timestamp, station, longValue, isMonthFormat);
	}
	
	public static String parseStation(Key key) {
		String rowKey = key.getRow().toString();
		return rowKey.split("_")[1];
	}
	
	public static boolean parseMonthFormat(Key key) {
		String rowKey = key.getRow().toString();
		String dateString = rowKey.split("_")[0];
		return (dateString.length() == 6) ? true : false;
	}
	
	public static long parseTimestamp(Key key) {
		String rowKey = key.getRow().toString();
		String dateString = rowKey.split("_")[0];
		boolean isMonthFormat = (dateString.length() == 6) ? true : false;
		if (isMonthFormat) {
			YearMonth date = YearMonth.parse(dateString, TimeFormatUtils.YEAR_MONTH);
			return date.atDay((int)key.getTimestamp()).toEpochDay();
		} else {
			Year date = Year.parse(dateString, TimeFormatUtils.YEAR);
			return date.atDay((int)key.getTimestamp()).toEpochDay();
		}
	}
	
	public static long parseValue(Value value) {
		return ByteBuffer.wrap(value.get()).getLong();
	}
	
	public static String parseMetricName(Key key) {
		return key.getColumnFamily().toString();
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

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	public boolean isMonthFormat() {
		return isMonthFormat;
	}

	public void setMonthFormat(boolean isMonthFormat) {
		this.isMonthFormat = isMonthFormat;
	}
	
}
