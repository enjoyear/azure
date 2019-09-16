package com.chen.guo.fun

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory}

import org.slf4j.{Logger, LoggerFactory}

object Fun extends App {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  logger.info(System.getenv("JAVA_HOME"))

  private val threadId = new AtomicInteger(0)
  private val threadPool = Executors.newFixedThreadPool(3, new ThreadFactory {
    override def newThread(r: Runnable) = new Thread(r, s"Thread-${threadId.incrementAndGet()}")
  })

  threadPool.submit(new SleepTask(2000))
  threadPool.submit(new SleepTask(5000))
  threadPool.submit(new SleepTask(2000))
  threadPool.submit(new SleepTask(3000))

  threadPool.shutdown()

  class SleepTask(sleep: Long) extends Runnable {
    override def run(): Unit = {
      logger.info(s"Thread ${Thread.currentThread().getName} sleeping for $sleep milliseconds...")
      Thread.sleep(sleep)
      logger.info(s"Thread ${Thread.currentThread().getName} wakes up after $sleep milliseconds")
    }
  }

}
