package com.chen.guo.scheduler;


import com.chen.guo.scheduler.db.DBQueryExecutioner;
import com.chen.guo.scheduler.job.QuartzJobDesc;
import com.chen.guo.util.PropertyFileLoader;
import lombok.extern.slf4j.Slf4j;
import org.junit.*;
import org.quartz.SchedulerException;

import java.io.File;
import java.util.Properties;

@Slf4j
public class MyQuartzSchedulerTest {
  private static DBQueryExecutioner queryExecutioner;
  private static MyQuartzScheduler scheduler;

  @BeforeClass
  public static void setupTestSchema() throws Exception {
    queryExecutioner = new TestDBUtil().getDBQueryExecutioner();
    String quartzFile = new File("src/test/resources/quartz.properties").getAbsolutePath();
    Properties props = PropertyFileLoader.load(quartzFile);

    scheduler = new MyQuartzScheduler(props);
    scheduler.start();
  }

  @AfterClass
  public static void tearDownTestSchema() {
    try {
      scheduler.shutdown();
      queryExecutioner.update("drop database quartz");
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @Before
  public void init() {
    //TestQuartzJob.COUNT_EXECUTION = 0;
  }

  @After
  public void cleanup() throws SchedulerException {
    scheduler.cleanup();
  }

  @Test
  public void testCreateScheduleAndRun() throws Exception {
    scheduler.scheduleJobIfAbsent("* * * * * ?", createJobDescription());
//    assertThat(scheduler.ifJobExist("SampleJob", "SampleService")).isEqualTo(true);
//    TestUtils.await().untilAsserted(() -> assertThat(TestQuartzJob.COUNT_EXECUTION)
//        .isNotNull().isGreaterThan(1));
  }

  private QuartzJobDesc createJobDescription() {
    return new QuartzJobDesc<>(TestQuartzJob.class, "SampleJob", "SampleService");
  }
}
