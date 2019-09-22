package com.chen.guo.scheduler;


import org.quartz.Job;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

/**
 * Produce Guice-able Job in this custom defined Job Factory.
 * <p>
 * In order to allow Quaratz jobs easily inject dependency, we create this factory. Every Quartz job
 * will be constructed by newJob method.
 */
public class SchedulerJobFactory implements JobFactory {

  public SchedulerJobFactory() {

  }

  @Override
  public Job newJob(final TriggerFiredBundle bundle, final Scheduler scheduler)
      throws SchedulerException {
    return null;
  }
}
