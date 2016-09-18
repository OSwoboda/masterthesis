package de.oswoboda.aggregation;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Calendar;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metric {
	
	private static final Logger LOG = LoggerFactory.getLogger(Metric.class);
	
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
		LOG.error("Long: "+ByteBuffer.wrap(value.get()).getLong());
		LOG.error("Double: "+ByteBuffer.wrap(value.get()).getDouble());
		double doubleValue = ByteBuffer.wrap(value.get()).getLong();
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
