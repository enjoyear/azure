package com.chen.guo.fun.concurrent;


import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.locks.ReentrantLock;


/**
 * https://en.wikipedia.org/wiki/Cigarette_smokers_problem
 */
@Slf4j
public class CigaretteSmokers {
  public static void main(String[] args) {
    //java "-Dmy.key=systemPropertyValue" -cp /Users/chguo/Documents/GitHub/azure/fun/build/libs/fun-1.0-SNAPSHOT.jar com.chen.guo.fun.concurrent.CigaretteSmokers
    log.info(System.getenv("JAVA_HOME"));
    log.info(System.getProperty("my.key"));
  }

  private final ReentrantLock paper = new ReentrantLock();
}
