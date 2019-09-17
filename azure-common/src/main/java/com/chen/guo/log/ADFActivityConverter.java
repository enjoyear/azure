package com.chen.guo.log;

import org.apache.log4j.helpers.PatternConverter;
import org.apache.log4j.spi.LoggingEvent;

public class ADFActivityConverter extends PatternConverter {

  @Override
  protected String convert(LoggingEvent event) {
    return "Activity";
  }

}
