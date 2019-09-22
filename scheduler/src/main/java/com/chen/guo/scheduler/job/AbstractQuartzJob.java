package com.chen.guo.scheduler.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.io.Serializable;

public abstract class AbstractQuartzJob implements Job {

  /**
   * Cast the object to the original one with the type. The object must extend Serializable.
   */
  protected static <T extends Serializable> T asT(final Object service, final Class<T> type) {
    return type.cast(service);
  }

  protected Object getKey(final JobExecutionContext context, final String key) {
    return context.getMergedJobDataMap().get(key);
  }
}
