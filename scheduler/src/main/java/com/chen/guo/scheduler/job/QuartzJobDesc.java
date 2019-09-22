package com.chen.guo.scheduler.job;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class QuartzJobDesc<T extends AbstractQuartzJob> {

  private final String groupName;
  private final String jobName;
  private final Class<T> jobClass;
  private final Map<String, ? extends Serializable> contextMap;

  public QuartzJobDesc(final Class<T> jobClass,
                       final String jobName, final String groupName,
                       final Map<String, ? extends Serializable> contextMap) {

    /**
     * This check is necessary for raw type. Please see test
     * {@link QuartzJobDescriptionTest#testCreateQuartzJobDescription2}
     */
    if (jobClass.getSuperclass() != AbstractQuartzJob.class) {
      throw new ClassCastException("jobClass must extend AbstractQuartzJob class");
    }
    this.jobClass = jobClass;
    this.jobName = jobName;
    this.groupName = groupName;
    this.contextMap = contextMap;
  }

  public QuartzJobDesc(final Class<T> jobClass,
                       final String jobName, final String groupName) {
    this(jobClass, jobName, groupName, new HashMap<String, String>());
  }

  public String getJobName() {
    return jobName;
  }

  public Class<? extends AbstractQuartzJob> getJobClass() {
    return this.jobClass;
  }

  public Map<String, ? extends Serializable> getContextMap() {
    return this.contextMap;
  }

  @Override
  public String toString() {
    return "QuartzJobDesc{" +
        "jobClass=" + this.jobClass +
        ", groupName='" + this.groupName + '\'' +
        ", contextMap=" + this.contextMap +
        '}';
  }

  public String getGroupName() {
    return this.groupName;
  }
}
