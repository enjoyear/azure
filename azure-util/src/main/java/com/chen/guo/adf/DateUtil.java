package com.chen.guo.adf;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;


public class DateUtil {
  public static final TimeZone LOCAL_TIMEZONE = TimeZone.getTimeZone("America/Los_Angeles");
  public static final TimeZone UTC_TIMEZONE = TimeZone.getTimeZone("UTC");

  private DateUtil() {
  }

  public static String localToUTCDatetimeString(int year, int month, int date, int hourOfDay, int minute, int second) {
    Calendar calendar = DateUtil.getLocalCalendar();
    calendar.set(year, month, date, hourOfDay, minute, second);
//    calendar.set(Calendar.YEAR, 2020);
//    calendar.set(Calendar.MONTH, 2);
//    calendar.set(Calendar.DAY_OF_MONTH, 19);
//    calendar.set(Calendar.HOUR_OF_DAY, 10);
//    calendar.set(Calendar.MINUTE, 0);
//    calendar.set(Calendar.SECOND, 0);
//    calendar.set(Calendar.MILLISECOND, 0);
    Timestamp now = new Timestamp(calendar.getTimeInMillis());
    return DateUtil.toDateString(now);
  }

  public static String toDateString(Timestamp date) {
    if (date == null) {
      return null;
    }
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    dateFormat.setTimeZone(UTC_TIMEZONE);
    return dateFormat.format(date);
  }

  public static Calendar getLocalCalendar() {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(LOCAL_TIMEZONE);
    return calendar;
  }
}