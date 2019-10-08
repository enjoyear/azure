package com.chen.guo;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;

@Slf4j
public class Fun {
  public static void main(String[] args) {

    Class klass = String.class;
    URL location = klass.getResource('/' + klass.getName().replace('.', '/') + ".class");

    log.info(System.getenv(location.toString()));
  }
}
