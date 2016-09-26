package de.oswoboda.aggregation;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeFormatUtils {
	
	public final static DateFormat YEAR_MONTH_DAY = new SimpleDateFormat("yyyyMMdd");
	public final static DateFormat YEAR_MONTH = new SimpleDateFormat("yyyyMM");
	public final static DateFormat YEAR = new SimpleDateFormat("yyyy");
	public final static DateFormat MONTH_DAY = new SimpleDateFormat("MMdd");
	public final static DateFormat MONTH = new SimpleDateFormat("MM");	
	public final static DateFormat DAY = new SimpleDateFormat("dd");
	
	public static Date parse(String dateString, DateFormat dateFormat) throws ParseException {
		return dateFormat.parse(dateString);
	}
	
	public static Date add(Date date, boolean bymonth, int amount) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(bymonth ? Calendar.DAY_OF_MONTH : Calendar.YEAR, amount);
		return calendar.getTime();
	}

}
