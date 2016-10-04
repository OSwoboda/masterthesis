package de.oswoboda;

import java.time.format.DateTimeFormatter;

public class TimeFormatUtils {
	
	public final static DateTimeFormatter YEAR_MONTH = DateTimeFormatter.ofPattern("yyyyMM");
	public final static DateTimeFormatter YEAR = DateTimeFormatter.ofPattern("yyyy");
	public final static DateTimeFormatter MONTH_DAY= DateTimeFormatter.ofPattern("MMdd");
	public final static DateTimeFormatter MONTH = DateTimeFormatter.ofPattern("MM");	
	public final static DateTimeFormatter DAY = DateTimeFormatter.ofPattern("dd");
	public final static DateTimeFormatter OUTPUT = DateTimeFormatter.ofPattern("HH:mm:ss,SSS");

}
