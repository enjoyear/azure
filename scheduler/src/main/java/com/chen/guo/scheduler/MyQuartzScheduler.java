package com.chen.guo.scheduler;


import com.chen.guo.scheduler.job.QuartzJobDesc;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.List;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

@Slf4j
public class MyQuartzScheduler {
  private final Scheduler _scheduler;

  public MyQuartzScheduler(Properties quartzProp) throws SchedulerException {
    StdSchedulerFactory schedulerFactory = new StdSchedulerFactory(quartzProp);
    _scheduler = schedulerFactory.getScheduler();
    // _scheduler.setJobFactory(new SchedulerJobFactory());
  }

  public void start() throws SchedulerException {
    _scheduler.start();
    log.info("Quartz Scheduler started.");
  }

  @VisibleForTesting
  void cleanup() throws SchedulerException {
    _scheduler.clear();
  }

  public void shutdown() throws SchedulerException {
    _scheduler.shutdown();
    log.info("Quartz Scheduler shut down.");
  }

  /**
   * Pause a job if it's present.
   *
   * @param jobName
   * @param groupName
   * @return true if job has been paused, no if job doesn't exist.
   * @throws SchedulerException
   */
  public synchronized boolean pauseJobIfPresent(String jobName, String groupName)
      throws SchedulerException {
    if (ifJobExist(jobName, groupName)) {
      _scheduler.pauseJob(new JobKey(jobName, groupName));
      return true;
    } else {
      return false;
    }
  }

  /**
   * Check if job is paused.
   *
   * @return true if job is paused, false otherwise.
   */
  public synchronized boolean isJobPaused(final String jobName, final String groupName)
      throws SchedulerException {
    if (!ifJobExist(jobName, groupName)) {
      throw new SchedulerException(String.format("Job (job name %s, group name %s) doesn't "
          + "exist'", jobName, groupName));
    }
    final JobKey jobKey = new JobKey(jobName, groupName);
    final JobDetail jobDetail = _scheduler.getJobDetail(jobKey);
    final List<? extends Trigger> triggers = _scheduler.getTriggersOfJob(jobDetail.getKey());
    for (final Trigger trigger : triggers) {
      final Trigger.TriggerState triggerState = _scheduler.getTriggerState(trigger.getKey());
      if (Trigger.TriggerState.PAUSED.equals(triggerState)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Resume a job.
   *
   * @param jobName
   * @param groupName
   * @return true the job has been resumed, no if the job doesn't exist.
   * @throws SchedulerException
   */
  public synchronized boolean resumeJobIfPresent(final String jobName, final String groupName)
      throws SchedulerException {
    if (ifJobExist(jobName, groupName)) {
      _scheduler.resumeJob(new JobKey(jobName, groupName));
      return true;
    } else {
      return false;
    }
  }

  /**
   * Unschedule a job.
   *
   * @param jobName
   * @param groupName
   * @return true if job is found and unscheduled.
   * @throws SchedulerException
   */
  public synchronized boolean unscheduleJob(final String jobName, final String groupName) throws
      SchedulerException {
    return _scheduler.deleteJob(new JobKey(jobName, groupName));
  }

  public synchronized boolean scheduleJobIfAbsent(final String cronExpression, final QuartzJobDesc
      jobDescription) throws SchedulerException {

    requireNonNull(jobDescription, "jobDescription is null");

    if (ifJobExist(jobDescription.getJobName(), jobDescription.getGroupName())) {
      log.warn(String.format("can not register existing job with job name: "
          + "%s and group name: %s", jobDescription.getJobName(), jobDescription.getGroupName()));
      return false;
    }

    if (!CronExpression.isValidExpression(cronExpression)) {
      throw new SchedulerException(
          "The cron expression string <" + cronExpression + "> is not valid.");
    }

    final JobDetail job = JobBuilder.newJob(jobDescription.getJobClass())
        .withIdentity(jobDescription.getJobName(), jobDescription.getGroupName()).build();

    // Add external dependencies to Job Data Map.
    job.getJobDataMap().putAll(jobDescription.getContextMap());

    final Trigger trigger = TriggerBuilder
        .newTrigger()
        .withSchedule(
            CronScheduleBuilder.cronSchedule(cronExpression)
                .withMisfireHandlingInstructionFireAndProceed()
//            .withMisfireHandlingInstructionDoNothing()
//            .withMisfireHandlingInstructionIgnoreMisfires()
        )
        .build();

    _scheduler.scheduleJob(job, trigger);
    log.info("Quartz Schedule with jobDetail " + job.getDescription() + " is registered.");
    return true;
  }


  @VisibleForTesting
  boolean ifJobExist(final String jobName, final String groupName)
      throws SchedulerException {
    return _scheduler.getJobDetail(new JobKey(jobName, groupName)) != null;
  }

  public Scheduler getScheduler() {
    return _scheduler;
  }
}
