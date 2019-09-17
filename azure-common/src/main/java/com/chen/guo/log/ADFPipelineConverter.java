package com.chen.guo.log;

import org.apache.log4j.helpers.PatternConverter;
import org.apache.log4j.spi.LoggingEvent;

public class ADFPipelineConverter extends PatternConverter {

  public static transient String pipelineRunId = "p_id";

  @Override
  protected String convert(LoggingEvent event) {
    return pipelineRunId;
  }
}
