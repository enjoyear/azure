package com.chen.guo;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Fun {
  public static void main(String[] args) {
    log.info(System.getenv("JAVA_HOME"));
  }
}
