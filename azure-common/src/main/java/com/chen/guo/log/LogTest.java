package com.chen.guo.log;

public class LogTest {
  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(LogTest.class);


  public static void main(String[] args) {
    log.info(System.getenv("JAVA_HOME"));
    log.error(System.getProperty("my.key"));
  }

}
