package com.chen.guo.log;

import org.apache.log4j.MDC;

public class LogTest {
  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(LogTest.class);


  public static void main(String[] args) {
    MDC.put("pipeline_runid", 123);
    MDC.put("activity_runid", 456);

    log.info("Hello");
    log.error("World");
  }
}
