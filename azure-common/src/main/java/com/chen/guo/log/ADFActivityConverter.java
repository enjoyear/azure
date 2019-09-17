package com.chen.guo.log;

import org.apache.log4j.helpers.PatternConverter;
import org.apache.log4j.spi.LoggingEvent;

public class ADFActivityConverter extends PatternConverter {

  public static transient String activityRunId = "a_id";

  @Override
  protected String convert(LoggingEvent event) {
    return activityRunId;
  }
}
