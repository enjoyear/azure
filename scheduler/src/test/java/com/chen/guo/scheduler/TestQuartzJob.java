package com.chen.guo.scheduler;

import com.chen.guo.scheduler.job.AbstractQuartzJob;
import org.quartz.JobExecutionContext;

public class TestQuartzJob extends AbstractQuartzJob {

  public static final String DELEGATE_CLASS_NAME = "SampleService";
  public static int COUNT_EXECUTION = 0;

  @Override
  public void execute(final JobExecutionContext context) {
//    final SampleService service = asT(getKey(context, DELEGATE_CLASS_NAME), SampleService.class);
    COUNT_EXECUTION++;
    System.out.println("Test quartz job executing");
  }
}
