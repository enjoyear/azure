package com.chen.guo.fun.concurrent;


import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * https://en.wikipedia.org/wiki/Cigarette_smokers_problem
 */
public class CigaretteSmokers {
  public static void main(String[] args) {

  }

  private final ReentrantLock paper = new ReentrantLock();
}
